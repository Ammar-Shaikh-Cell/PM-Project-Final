"""
Machine State Detection Service

Implements intelligent machine state detection with 5 states:
- OFF: Machine off / cold
- HEATING: Warming up, not producing
- IDLE: Warm and ready, but not producing
- PRODUCTION: Active process (traffic light + baseline + anomalies enabled)
- COOLING: Cooling down, not producing

Process evaluation (traffic-light, baseline, anomalies) only runs in PRODUCTION.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from collections import deque
import statistics
from loguru import logger

class MachineState(Enum):
    """Machine operating states"""
    OFF = "OFF"
    HEATING = "HEATING"
    IDLE = "IDLE"
    PRODUCTION = "PRODUCTION"
    COOLING = "COOLING"

@dataclass
class StateThresholds:
    """Configurable thresholds for state detection"""
    # Core thresholds
    RPM_ON: float = 5.0          # rpm - movement present
    RPM_PROD: float = 10.0       # rpm - production possible (inclusive: >= 10.0)
    P_ON: float = 2.0            # bar - pressure present
    P_PROD: float = 5.0          # bar - typical production pressure
    T_MIN_ACTIVE: float = 60.0   # °C - below this = cold/off
    
    # Temperature rate thresholds
    HEATING_RATE: float = 0.2    # °C/min - positive heating
    COOLING_RATE: float = -0.2   # °C/min - negative cooling
    
    # Stability thresholds
    TEMP_FLAT_RATE: float = 0.2  # °C/min - considered flat
    RPM_STABLE_MAX: float = 2.0  # rpm std dev for stable
    PRESSURE_STABLE_MAX: float = 1.0  # bar std dev for stable
    
    # Hysteresis timers (seconds)
    PRODUCTION_ENTER_TIME: int = 90    # seconds
    PRODUCTION_EXIT_TIME: int = 120    # seconds
    STATE_CHANGE_DEBOUNCE: int = 60    # seconds
    
    # Optional thresholds
    MOTOR_LOAD_MIN: float = 0.15   # 15% for production fallback
    THROUGHPUT_MIN: float = 0.1    # kg/h for production fallback

@dataclass
class SensorReading:
    """Single sensor reading with timestamp"""
    timestamp: datetime
    screw_rpm: Optional[float] = None
    pressure_bar: Optional[float] = None
    temp_zone_1: Optional[float] = None
    temp_zone_2: Optional[float] = None
    temp_zone_3: Optional[float] = None
    temp_zone_4: Optional[float] = None
    motor_load: Optional[float] = None
    throughput_kg_h: Optional[float] = None
    line_enable: Optional[bool] = None
    heater_on: Optional[bool] = None
    heater_power: Optional[float] = None

@dataclass
class DerivedMetrics:
    """Derived metrics from sensor readings"""
    temp_avg: Optional[float] = None
    temp_spread: Optional[float] = None
    d_temp_avg: Optional[float] = None  # °C/min
    rpm_stable: Optional[float] = None
    pressure_stable: Optional[float] = None
    any_temp_above_min: bool = False
    all_temps_below: bool = True

@dataclass
class MachineStateInfo:
    """Current machine state with metadata"""
    state: MachineState
    confidence: float  # 0.0 - 1.0
    state_since: datetime
    last_updated: datetime
    metrics: DerivedMetrics
    flags: Dict[str, Any] = None
    state_duration_seconds: Optional[float] = None

class StateTimer:
    """Manages hysteresis timers for state transitions"""
    def __init__(self):
        self.timers: Dict[str, datetime] = {}
        self.state_start_times: Dict[MachineState, datetime] = {}
    
    def start_timer(self, timer_name: str, duration_seconds: int) -> datetime:
        """Start a timer and return expiry time"""
        expiry = datetime.utcnow() + timedelta(seconds=duration_seconds)
        self.timers[timer_name] = expiry
        return expiry
    
    def is_timer_expired(self, timer_name: str) -> bool:
        """Check if timer has expired"""
        if timer_name not in self.timers:
            return True
        return datetime.utcnow() >= self.timers[timer_name]
    
    def clear_timer(self, timer_name: str):
        """Clear a timer"""
        self.timers.pop(timer_name, None)
    
    def set_state_start(self, state: MachineState):
        """Record when a state started"""
        self.state_start_times[state] = datetime.utcnow()
    
    def get_state_duration(self, state: MachineState) -> timedelta:
        """Get how long current state has been active"""
        if state not in self.state_start_times:
            return timedelta(0)
        return datetime.utcnow() - self.state_start_times[state]

class MachineStateDetector:
    """Main machine state detection service"""
    
    def __init__(self, machine_id: str, thresholds: Optional[StateThresholds] = None):
        self.machine_id = machine_id
        self.thresholds = thresholds or StateThresholds()
        self.timer = StateTimer()
        
        # Data buffers for calculations
        # reading_buffer: 10 minutes for stability metrics (assuming 1-second intervals = 600 samples)
        self.reading_buffer: deque = deque(maxlen=600)  # 10 minutes for stability metrics
        self.temp_history: deque = deque(maxlen=300)   # 5 minutes for temperature slope
        
        # Current state - default to OFF when no data available
        self.current_state: MachineStateInfo = MachineStateInfo(
            state=MachineState.OFF,  # Default to OFF (machine is turned off)
            confidence=0.5,  # Medium confidence for default state
            state_since=datetime.utcnow(),
            last_updated=datetime.utcnow(),
            metrics=DerivedMetrics(),
            state_duration_seconds=0.0
        )
        
        logger.info(f"Machine state detector initialized for {machine_id}")
    
    def add_reading(self, reading: SensorReading) -> MachineStateInfo:
        """Add new sensor reading and update state"""
        try:
            # Add to buffers
            self.reading_buffer.append(reading)
            
            # Calculate derived metrics
            metrics = self._calculate_derived_metrics(reading)
            
            # Check for sensor faults - if detected, default to OFF state
            if self._detect_sensor_fault(reading, metrics):
                # Sensor fault detected - return OFF state with low confidence
                logger.warning(f"Sensor fault detected for {self.machine_id}, defaulting to OFF state")
                new_state = MachineState.OFF
                confidence = 0.3
            else:
                # Determine new state
                new_state, confidence = self._determine_state(reading, metrics)
            
            # Apply hysteresis/debounce
            final_state, final_confidence = self._apply_hysteresis(new_state, confidence)
            
            # Update current state if changed
            if final_state != self.current_state.state:
                self.current_state.state = final_state
                self.current_state.confidence = final_confidence
                self.current_state.state_since = datetime.utcnow()
                self.current_state.state_duration_seconds = 0.0
                self.timer.set_state_start(final_state)
                
                logger.info(f"Machine {self.machine_id} state changed: {final_state.value}")
            
            self.current_state.last_updated = datetime.utcnow()
            self.current_state.metrics = metrics
            
            # Update state duration
            duration = self.timer.get_state_duration(self.current_state.state)
            self.current_state.state_duration_seconds = duration.total_seconds()
            
            return self.current_state
            
        except Exception as e:
            logger.error(f"Error processing reading for {self.machine_id}: {e}")
            # Return OFF state on error with low confidence
            self.current_state.state = MachineState.OFF
            self.current_state.confidence = 0.2
            self.current_state.last_updated = datetime.utcnow()
            return self.current_state
    
    def _calculate_derived_metrics(self, reading: SensorReading) -> DerivedMetrics:
        """Calculate derived metrics from sensor reading"""
        # Temperature metrics
        temps = [reading.temp_zone_1, reading.temp_zone_2, reading.temp_zone_3, reading.temp_zone_4]
        valid_temps = [t for t in temps if t is not None]
        
        temp_avg = statistics.mean(valid_temps) if valid_temps else None
        temp_spread = max(valid_temps) - min(valid_temps) if len(valid_temps) >= 2 else None
        
        # Temperature slope (°C/min) - need historical data
        d_temp_avg = self._calculate_temperature_slope(temp_avg)
        
        # Stability metrics (std dev over last 10 minutes)
        rpm_stable = self._calculate_stability_metric('screw_rpm')
        pressure_stable = self._calculate_stability_metric('pressure_bar')
        
        # Convenience flags
        any_temp_above_min = any(t > self.thresholds.T_MIN_ACTIVE for t in valid_temps) if valid_temps else False
        all_temps_below = all(t < self.thresholds.T_MIN_ACTIVE for t in valid_temps) if valid_temps else True
        
        return DerivedMetrics(
            temp_avg=temp_avg,
            temp_spread=temp_spread,
            d_temp_avg=d_temp_avg,
            rpm_stable=rpm_stable,
            pressure_stable=pressure_stable,
            any_temp_above_min=any_temp_above_min,
            all_temps_below=all_temps_below
        )
    
    def _calculate_temperature_slope(self, current_temp: Optional[float]) -> Optional[float]:
        """Calculate temperature slope in °C/min"""
        if current_temp is None:
            return None
        
        # Add current temperature to history
        self.temp_history.append((datetime.utcnow(), current_temp))
        
        # Need at least 2 minutes of data for meaningful slope
        if len(self.temp_history) < 120:
            return None
        
        # Calculate slope between current and 5-6 minutes ago
        now = datetime.utcnow()
        five_min_ago = now - timedelta(minutes=5)
        six_min_ago = now - timedelta(minutes=6)
        
        # Find average temperature in 5-6 minute window
        historical_temps = [
            temp for timestamp, temp in self.temp_history
            if six_min_ago <= timestamp <= five_min_ago
        ]
        
        if not historical_temps:
            return None
        
        historical_avg = statistics.mean(historical_temps)
        
        # Calculate slope (°C/min)
        time_diff_min = 5.0  # 5 minutes difference
        slope = (current_temp - historical_avg) / time_diff_min
        
        return slope
    
    def _calculate_stability_metric(self, field_name: str) -> Optional[float]:
        """Calculate standard deviation over last 10 minutes"""
        if len(self.reading_buffer) < 10:  # Need minimum samples
            return None
        
        # Get last 10 minutes of data
        now = datetime.utcnow()
        ten_min_ago = now - timedelta(minutes=10)
        
        values = []
        for reading in self.reading_buffer:
            if reading.timestamp >= ten_min_ago:
                value = getattr(reading, field_name, None)
                if value is not None:
                    values.append(value)
        
        if len(values) < 10:  # Need minimum samples for stability (at least 10 samples over 10 minutes)
            return None
        
        return statistics.stdev(values) if len(values) > 1 else 0.0
    
    def _detect_sensor_fault(self, reading: SensorReading, metrics: DerivedMetrics) -> bool:
        """Detect sensor faults and invalid data"""
        # Check for implausible temperatures
        temps = [reading.temp_zone_1, reading.temp_zone_2, reading.temp_zone_3, reading.temp_zone_4]
        valid_temps = [t for t in temps if t is not None]
        
        # Temperature faults - check for implausible values
        if valid_temps:
            if any(t <= 0 or t < -20 for t in valid_temps):
                logger.warning(f"Implausible temperature detected for {self.machine_id}: {valid_temps}")
                return True
            if any(t > 400 for t in valid_temps):  # Unlikely for extruder
                logger.warning(f"Temperature too high for {self.machine_id}: {valid_temps}")
                return True
        
        # Pressure fault (exactly 0 while RPM is high) - indicates sensor issue
        if (reading.pressure_bar is not None and reading.pressure_bar == 0 and 
            reading.screw_rpm is not None and reading.screw_rpm > self.thresholds.RPM_PROD):
            logger.warning(f"Pressure fault: RPM={reading.screw_rpm} but pressure=0 for {self.machine_id}")
            return True
        
        # Missing critical data - RPM is essential
        if reading.screw_rpm is None:
            logger.debug(f"Missing RPM data for {self.machine_id} - sensor fault")
            return True
        
        # Too many missing temperature zones - need at least 2 for reliable state detection
        if len(valid_temps) < 2:  # At least 2 zones needed
            logger.debug(f"Insufficient temperature zones ({len(valid_temps)}/4) for {self.machine_id} - sensor fault")
            return True
        
        # Invalid timestamp - check for future timestamps (timezone issues)
        # Allow up to 24 hours in the future (to handle timezone differences)
        # Just log a warning but don't treat as fault if sensor values are valid
        if reading.timestamp > datetime.utcnow() + timedelta(hours=24):
            logger.warning(f"Timestamp too far in future for {self.machine_id}: {reading.timestamp} (current: {datetime.utcnow()})")
            return True
        elif reading.timestamp > datetime.utcnow() + timedelta(minutes=1):
            # Future timestamp but within reasonable range (likely timezone issue)
            # Log warning but don't treat as fault - sensor values are still valid
            logger.debug(f"Future timestamp detected for {self.machine_id}: {reading.timestamp} (current: {datetime.utcnow()}) - likely timezone issue, continuing with state determination")
            # Don't return True - allow state determination to proceed
        
        return False
    
    def _determine_state(self, reading: SensorReading, metrics: DerivedMetrics) -> Tuple[MachineState, float]:
        """Determine machine state based on current readings"""
        # Check for missing critical data - don't convert None to 0.0
        # This allows us to distinguish "machine off" (0.0) from "no data" (None)
        rpm = reading.screw_rpm
        pressure = reading.pressure_bar
        temp_avg = metrics.temp_avg
        d_temp = metrics.d_temp_avg
        
        # If critical data is missing, we can't determine state accurately
        # This should have been caught by _detect_sensor_fault, but handle it here too
        if rpm is None:
            logger.warning(f"Missing RPM data for {self.machine_id}, defaulting to OFF state")
            return MachineState.OFF, 0.3
        
        # Use 0.0 for comparisons only when we have valid None handling
        rpm_val = rpm if rpm is not None else 0.0
        pressure_val = pressure if pressure is not None else 0.0
        temp_avg_val = temp_avg if temp_avg is not None else 0.0
        
        logger.info("State determination: machine_id={}, rpm={}, pressure={}, temp_avg={}, d_temp={}, thresholds: RPM_PROD={}, P_PROD={}", 
                    self.machine_id, rpm_val, pressure_val, temp_avg_val, d_temp, 
                    self.thresholds.RPM_PROD, self.thresholds.P_PROD)
        
        # PRODUCTION: primary criteria - CHECK FIRST before other states
        # This ensures high RPM + high pressure = PRODUCTION, regardless of other conditions
        if (rpm_val >= self.thresholds.RPM_PROD and 
            pressure is not None and pressure >= self.thresholds.P_PROD):
            logger.info("✅ PRODUCTION state detected (primary): machine_id={}, rpm={} (>= {}), pressure={} (>= {}), temp_avg={}", 
                       self.machine_id, rpm_val, self.thresholds.RPM_PROD, pressure, self.thresholds.P_PROD, temp_avg)
            return MachineState.PRODUCTION, 0.9
        else:
            # Log why PRODUCTION wasn't detected
            if rpm_val < self.thresholds.RPM_PROD:
                logger.debug("PRODUCTION not detected: rpm={} < RPM_PROD={}", rpm_val, self.thresholds.RPM_PROD)
            if pressure is None:
                logger.debug("PRODUCTION not detected: pressure is None")
            elif pressure < self.thresholds.P_PROD:
                logger.debug("PRODUCTION not detected: pressure={} < P_PROD={}", pressure, self.thresholds.P_PROD)
        
        # PRODUCTION: fallback criteria - also check before OFF/IDLE
        if rpm_val >= self.thresholds.RPM_PROD:
            fallback_conditions = []
            
            # Check pressure
            if pressure is not None and pressure >= self.thresholds.P_ON:
                fallback_conditions.append("pressure")
            
            # Check motor load
            if reading.motor_load is not None and reading.motor_load >= self.thresholds.MOTOR_LOAD_MIN:
                fallback_conditions.append("motor_load")
            
            # Check throughput
            if reading.throughput_kg_h is not None and reading.throughput_kg_h >= self.thresholds.THROUGHPUT_MIN:
                fallback_conditions.append("throughput")
            
            if fallback_conditions:
                confidence = 0.7 if len(fallback_conditions) > 1 else 0.6
                logger.info("PRODUCTION state detected (fallback): machine_id={}, rpm={}, conditions={}", 
                           self.machine_id, rpm_val, fallback_conditions)
                return MachineState.PRODUCTION, confidence
        
        # OFF: cold, no RPM, low/no pressure
        # IMPORTANT: Only return OFF if machine is COLD. If warm, check for IDLE instead.
        if rpm_val < self.thresholds.RPM_ON:
            # Machine is not running - check if it's cold (OFF) or warm (IDLE)
            if temp_avg is not None and temp_avg < self.thresholds.T_MIN_ACTIVE:
                # Machine is cold and not running - definitely OFF
                logger.debug("OFF state detected: machine_id={}, temp={}, rpm={}, pressure={}", 
                           self.machine_id, temp_avg, rpm_val, pressure_val)
                return MachineState.OFF, 0.9
            elif temp_avg is None and pressure_val < self.thresholds.P_ON:
                # No temperature data but low pressure and no RPM - likely OFF
                logger.debug("OFF state detected (no temp data): machine_id={}, rpm={}, pressure={}", 
                           self.machine_id, rpm_val, pressure_val)
                return MachineState.OFF, 0.7
            elif rpm_val == 0.0 and temp_avg is not None and temp_avg < self.thresholds.T_MIN_ACTIVE:
                # RPM is exactly 0 and cold - OFF even if pressure is slightly above threshold (residual pressure)
                logger.debug("OFF state detected (RPM=0, cold): machine_id={}, temp={}, pressure={}", 
                           self.machine_id, temp_avg, pressure_val)
                return MachineState.OFF, 0.85
            # If we reach here, machine is warm (temp_avg >= T_MIN_ACTIVE) but not running
            # Don't return OFF here - let IDLE detection handle it below
        
        # COOLING: RPM off, temperature falling
        # Require valid d_temp for COOLING detection
        if (rpm_val < self.thresholds.RPM_ON and 
            temp_avg is not None and temp_avg >= self.thresholds.T_MIN_ACTIVE and
            d_temp is not None and d_temp <= self.thresholds.COOLING_RATE):
            return MachineState.COOLING, 0.8
        
        # HEATING: temperature rising, no production
        # Require valid d_temp for HEATING detection
        if (rpm_val < self.thresholds.RPM_PROD and 
            temp_avg is not None and temp_avg >= self.thresholds.T_MIN_ACTIVE and
            d_temp is not None and d_temp >= self.thresholds.HEATING_RATE):
            return MachineState.HEATING, 0.8
        
        
        # IDLE: warm, stable, no production
        # PRIORITIZE IDLE when machine is warm (temp_avg >= T_MIN_ACTIVE) and not running
        # This should be checked BEFORE defaulting to OFF
        if (rpm_val < self.thresholds.RPM_ON and 
            temp_avg is not None and temp_avg >= self.thresholds.T_MIN_ACTIVE):
            # Machine is warm and not running - check if it's IDLE
            # Allow residual pressure (up to 1.5x P_ON) when RPM is 0, as pressure can linger after shutdown
            if pressure_val < (self.thresholds.P_ON * 1.5):  # Allow residual pressure (up to 3.0 bar when P_ON=2.0)
                # Check if we have d_temp for stability confirmation
                if d_temp is not None and abs(d_temp) < self.thresholds.TEMP_FLAT_RATE:
                    # Stable temperature - confirmed IDLE
                    logger.debug("IDLE state (warm, stable): machine_id={}, temp={}, rpm={}, pressure={}, d_temp={}", 
                               self.machine_id, temp_avg, rpm_val, pressure_val, d_temp)
                    return MachineState.IDLE, 0.8
                elif d_temp is None:
                    # No temperature slope data - but machine is warm, not running, low pressure
                    # This is most likely IDLE (warm and ready)
                    if len(self.temp_history) < 120:
                        # Not enough history yet - but still likely IDLE
                        logger.debug("IDLE state (warm, no history): machine_id={}, temp={}, rpm={}, pressure={}", 
                                   self.machine_id, temp_avg, rpm_val, pressure_val)
                        return MachineState.IDLE, 0.6
                    else:
                        # Have history but d_temp calculation failed - still likely IDLE
                        logger.debug("IDLE state (warm, d_temp unavailable): machine_id={}, temp={}, rpm={}, pressure={}", 
                                   self.machine_id, temp_avg, rpm_val, pressure_val)
                        return MachineState.IDLE, 0.6
                else:
                    # d_temp exists but indicates heating/cooling - check if it's significant
                    if abs(d_temp) >= self.thresholds.TEMP_FLAT_RATE:
                        # Temperature is changing - could be HEATING or COOLING (already checked above)
                        # But if those didn't match, it might be a small change - still likely IDLE
                        logger.debug("IDLE state (warm, slight temp change): machine_id={}, temp={}, d_temp={}", 
                                   self.machine_id, temp_avg, d_temp)
                        return MachineState.IDLE, 0.5
                # If we reach here, machine is warm, not running, low pressure - IDLE
                logger.debug("IDLE state (warm, fallback): machine_id={}, temp={}, rpm={}, pressure={}", 
                           self.machine_id, temp_avg, rpm_val, pressure_val)
                return MachineState.IDLE, 0.6
        
        # Default to OFF if we can't determine otherwise
        # This is safer than defaulting to IDLE
        logger.debug("Defaulting to OFF state: machine_id={}, rpm={}, pressure={}, temp_avg={}", 
                    self.machine_id, rpm_val, pressure_val, temp_avg)
        return MachineState.OFF, 0.4
    
    def _apply_hysteresis(self, new_state: MachineState, confidence: float) -> Tuple[MachineState, float]:
        """Apply hysteresis and debounce logic"""
        current = self.current_state.state
        
        # No change - return current state
        if new_state == current:
            return current, confidence
        
        # Special handling for PRODUCTION (requires 90s)
        if new_state == MachineState.PRODUCTION:
            timer_name = f"enter_production_{self.machine_id}"
            
            # If already in PRODUCTION, no change needed
            if current == MachineState.PRODUCTION:
                return new_state, confidence
            
            # Check if timer has expired (or doesn't exist)
            if self.timer.is_timer_expired(timer_name):
                # Timer expired or doesn't exist - check if we've been meeting PRODUCTION criteria
                # For first-time detection or after timer expiry, allow immediate transition
                # but only if we have enough readings to be confident
                if len(self.reading_buffer) >= 10:  # Need at least 10 readings for confidence
                    logger.info(f"PRODUCTION transition allowed: timer expired, {len(self.reading_buffer)} readings available")
                    self.timer.clear_timer(timer_name)
                    self.timer.set_state_start(new_state)
                    return new_state, confidence
                else:
                    # Not enough readings yet - start timer
                    logger.debug(f"PRODUCTION transition delayed: only {len(self.reading_buffer)} readings, need 10+")
                    if timer_name not in self.timer.timers:
                        self.timer.start_timer(timer_name, self.thresholds.PRODUCTION_ENTER_TIME)
                    return current, confidence
            else:
                # Timer still running - check how long we've been meeting PRODUCTION criteria
                # Count consecutive readings that meet PRODUCTION criteria
                production_readings = 0
                for r in list(self.reading_buffer)[-30:]:  # Check last 30 readings
                    if (r.screw_rpm is not None and r.screw_rpm >= self.thresholds.RPM_PROD and
                        r.pressure_bar is not None and r.pressure_bar >= self.thresholds.P_PROD):
                        production_readings += 1
                
                # If we have enough consecutive production readings, allow transition
                if production_readings >= 10:  # At least 10 consecutive production readings
                    logger.info(f"PRODUCTION transition allowed: {production_readings} consecutive production readings")
                    self.timer.clear_timer(timer_name)
                    self.timer.set_state_start(new_state)
                    return new_state, confidence
                else:
                    # Still waiting - return current state
                    logger.debug(f"PRODUCTION transition waiting: {production_readings}/10 consecutive production readings")
                    return current, confidence
        
        # Exiting production (requires 120s)
        elif current == MachineState.PRODUCTION and new_state != MachineState.PRODUCTION:
            timer_name = f"exit_production_{self.machine_id}"
            
            if not self.timer.is_timer_expired(timer_name):
                return current, confidence
            
            # Check if we've been out of production criteria for 120s
            # This is handled by checking recent readings in the state determination
            self.timer.clear_timer(timer_name)
            return new_state, confidence
        
        # Other state changes (60s debounce)
        else:
            timer_name = f"state_change_{self.machine_id}"
            
            if not self.timer.is_timer_expired(timer_name):
                return current, confidence
            
            self.timer.clear_timer(timer_name)
            return new_state, confidence
    
    def get_current_state(self) -> MachineStateInfo:
        """Get current machine state"""
        now = datetime.utcnow()
        time_since_update = (now - self.current_state.last_updated).total_seconds()
        
        # Check if we have any readings at all
        has_any_readings = len(self.reading_buffer) > 0
        
        # Check if we have recent readings (within last 2 minutes)
        has_recent_readings = False
        if has_any_readings:
            last_reading = self.reading_buffer[-1]
            if hasattr(last_reading, 'timestamp') and last_reading.timestamp:
                time_since_last_reading = (now - last_reading.timestamp).total_seconds()
                has_recent_readings = time_since_last_reading < 120  # 2 minutes
        
        # If no readings at all, or state is stale (no data for 5+ minutes), return OFF
        if not has_any_readings:
            # Never received any data - default to OFF state
            logger.debug(f"Machine {self.machine_id} has no readings - returning OFF state")
            return MachineStateInfo(
                state=MachineState.OFF,
                confidence=0.1,  # Very low confidence - no data available
                state_since=self.current_state.state_since,
                last_updated=now,
                metrics=DerivedMetrics(),
                flags={"no_data": True, "last_known_state": self.current_state.state.value},
                state_duration_seconds=time_since_update
            )
        
        # If state hasn't been updated in 5 minutes and we have no recent readings, it's stale
        if time_since_update > 300 and not has_recent_readings:  # 5 minutes
            # State is stale - no data coming, default to OFF
            logger.warning(f"Machine {self.machine_id} state is stale (last update: {time_since_update:.0f}s ago, no recent readings) - defaulting to OFF")
            return MachineStateInfo(
                state=MachineState.OFF,
                confidence=0.2,  # Very low confidence - we don't know the actual state
                state_since=self.current_state.state_since,
                last_updated=now,
                metrics=DerivedMetrics(),
                flags={"stale": True, "last_known_state": self.current_state.state.value},
                state_duration_seconds=time_since_update
            )
        
        return self.current_state
    
    def is_in_production(self) -> bool:
        """Check if machine is currently in PRODUCTION state"""
        return self.current_state.state == MachineState.PRODUCTION
    
    def get_state_duration(self) -> timedelta:
        """Get duration of current state"""
        return self.timer.get_state_duration(self.current_state.state)

# Global registry for machine state detectors
_machine_detectors: Dict[str, MachineStateDetector] = {}

def get_machine_detector(machine_id: str, thresholds: Optional[StateThresholds] = None) -> MachineStateDetector:
    """Get or create machine state detector for a machine"""
    if machine_id not in _machine_detectors:
        _machine_detectors[machine_id] = MachineStateDetector(machine_id, thresholds)
    return _machine_detectors[machine_id]

def remove_machine_detector(machine_id: str):
    """Remove machine state detector"""
    _machine_detectors.pop(machine_id, None)

def get_all_machine_states() -> Dict[str, MachineStateInfo]:
    """Get current states of all machines"""
    return {machine_id: detector.get_current_state() 
            for machine_id, detector in _machine_detectors.items()}

async def process_sensor_data_for_state(
    session, machine_id: str, sensor_type: str, value: float, timestamp: datetime
):
    """Process incoming sensor data for machine state detection"""
    try:
        logger.info(f"Entering process_sensor_data_for_state: machine_id={machine_id}, sensor_type={sensor_type}")
        
        # Use the global detector registry
        detector = get_machine_detector(machine_id)
        
        # Map sensor types to detector fields
        sensor_mapping = {
            'temperature': 'temp_zone_1',  # Use zone 1 for general temperature
            'temperature sensor': 'temp_zone_1',
            'pressure': 'pressure_bar',
            'pressure sensor': 'pressure_bar',
            'vibration': None,  # Not directly mapped, but could affect derived metrics
            'vibration sensor': None,
            'motor_current': 'motor_load',
            'motor current': 'motor_load',
            'motor current sensor': 'motor_load',
            'rpm': 'screw_rpm',
            'rpm sensor': 'screw_rpm',
            'speed sensor': 'screw_rpm',
            'load sensor': 'motor_load',
            'current sensor': 'motor_load',
            'torque sensor': None,  # Could be mapped to motor_load if needed
            'flow sensor': None,
            'oil level sensor': None
        }
        
        field_name = sensor_mapping.get(sensor_type.lower())
        logger.info(f"Processing sensor data: machine_id={machine_id}, sensor_type={sensor_type}, field_name={field_name}")
        
        if field_name:
            # Get current state and existing sensor data
            current_state = detector.get_current_state()
            
            # Create a new reading with the new value and existing values
            reading = SensorReading(timestamp=timestamp)
            
            # Copy existing values from current state metrics if available
            if current_state.metrics:
                if field_name != 'temp_zone_1' and current_state.metrics.temp_avg:
                    reading.temp_zone_1 = current_state.metrics.temp_avg
                if field_name != 'pressure_bar' and hasattr(current_state.metrics, 'pressure_bar') and current_state.metrics.pressure_bar:
                    reading.pressure_bar = current_state.metrics.pressure_bar
                if field_name != 'screw_rpm' and hasattr(current_state.metrics, 'screw_rpm') and current_state.metrics.screw_rpm:
                    reading.screw_rpm = current_state.metrics.screw_rpm
                if field_name != 'motor_load' and hasattr(current_state.metrics, 'motor_load') and current_state.metrics.motor_load:
                    reading.motor_load = current_state.metrics.motor_load
            
            # Update the specific field
            if field_name == 'temp_zone_1':
                reading.temp_zone_1 = value
            elif field_name == 'pressure_bar':
                reading.pressure_bar = value
            elif field_name == 'motor_load':
                reading.motor_load = value
            elif field_name == 'screw_rpm':
                reading.screw_rpm = value
            
            # Process the reading for state detection
            detector.add_reading(reading)
            
            # Store state in database if changed
            new_state = detector.get_current_state()
            if new_state.state != current_state.state:
                await store_machine_state_in_db(session, machine_id, new_state)
                logger.info(f"Machine {machine_id} state changed to {new_state.state.value}")
                
    except Exception as e:
        logger.error(f"Error processing sensor data for machine state: {e}")

async def store_machine_state_in_db(session, machine_id: str, state_info: MachineStateInfo):
    """Store machine state transition in database"""
    try:
        from app.models.machine_state import MachineState, MachineStateEnum
        
        # Create machine state record
        machine_state = MachineState(
            machine_id=machine_id,
            state=MachineStateEnum(state_info.state.value),
            confidence=state_info.confidence,
            state_since=state_info.state_since,
            last_updated=state_info.last_updated,
            metrics=state_info.metrics.__dict__ if state_info.metrics else {},
            flags=state_info.flags or {}
        )
        
        session.add(machine_state)
        await session.flush()  # Get ID without committing
        
        logger.info(f"Stored machine state transition: {machine_id} -> {state_info.state.value}")
        
    except Exception as e:
        logger.error(f"Error storing machine state in database: {e}")
