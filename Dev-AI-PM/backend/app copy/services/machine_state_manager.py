"""
Service layer for machine state management
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, desc, func
from sqlalchemy.orm import selectinload

from app.models.machine_state import (
    MachineState, MachineStateThresholds, MachineStateTransition, 
    MachineStateAlert, MachineProcessEvaluation, MachineStateEnum
)
from app.models.machine import Machine
from app.schemas.machine_state import (
    MachineStateInfo, MachineStateThresholds as ThresholdsSchema,
    MachineStateTransition as TransitionSchema, MachineStateAlert as AlertSchema,
    MachineProcessEvaluation as EvaluationSchema, MachineStateStatistics
)
from app.services.machine_state_service import (
    MachineStateDetector, get_machine_detector, remove_machine_detector,
    StateThresholds, SensorReading, get_all_machine_states
)

logger = logging.getLogger(__name__)


class MachineStateService:
    """Service for managing machine state detection and storage"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        # Use global detector registry instead of local one
    
    async def initialize_machine_detector(self, machine_id: str) -> MachineStateDetector:
        """Initialize or get machine state detector with custom thresholds"""
        # Get custom thresholds from database
        thresholds = await self.get_machine_thresholds(machine_id)
        
        if thresholds:
            # Convert database thresholds to service thresholds
            service_thresholds = StateThresholds(
                rpm_on=thresholds.rpm_on,
                rpm_prod=thresholds.rpm_prod,
                p_on=thresholds.p_on,
                p_prod=thresholds.p_prod,
                t_min_active=thresholds.t_min_active,
                heating_rate=thresholds.heating_rate,
                cooling_rate=thresholds.cooling_rate,
                temp_flat_rate=thresholds.temp_flat_rate,
                rpm_stable_max=thresholds.rpm_stable_max,
                pressure_stable_max=thresholds.pressure_stable_max,
                production_enter_time=thresholds.production_enter_time,
                production_exit_time=thresholds.production_exit_time,
                state_change_debounce=thresholds.state_change_debounce,
                motor_load_min=thresholds.motor_load_min,
                throughput_min=thresholds.throughput_min
            )
        else:
            # Use default thresholds
            service_thresholds = StateThresholds()
        
        detector = get_machine_detector(machine_id, service_thresholds)
        return detector
    
    async def process_sensor_reading(self, machine_id: str, reading: SensorReading) -> MachineStateInfo:
        """Process sensor reading and update machine state"""
        try:
            # Get detector from global registry
            detector = get_machine_detector(machine_id)
            
            # Get previous state
            previous_state = detector.get_current_state()
            
            # Process reading
            current_state = detector.add_reading(reading)
            
            # Log state transition if changed
            if previous_state.state != current_state.state:
                # Convert machine_id to string if it's a UUID
                machine_id_str = str(machine_id)
                
                # Convert MachineState enum to MachineStateEnum for database operations
                from_state_enum = MachineStateEnum(previous_state.state.value) if previous_state.state else None
                to_state_enum = MachineStateEnum(current_state.state.value)
                
                await self._log_state_transition(
                    machine_id_str, from_state_enum, to_state_enum,
                    previous_state, current_state, reading
                )
                
                # Store state in database
                await self._store_machine_state(machine_id_str, current_state, reading)
                
                # Handle state-based actions (including email notifications)
                await self._handle_state_actions(machine_id_str, from_state_enum, to_state_enum)
            
            return current_state
            
        except Exception as e:
            logger.error(f"Error processing sensor reading for {machine_id}: {e}")
            raise
    
    async def get_current_state(self, machine_id: str) -> Optional[MachineStateInfo]:
        """Get current machine state"""
        detector = get_machine_detector(machine_id)
        return detector.get_current_state()
    
    async def get_all_current_states(self) -> Dict[str, MachineStateInfo]:
        """Get current states of all machines"""
        # First, ensure all machines have detectors initialized
        await self._initialize_all_machine_detectors()
        
        # Use global detector registry
        return get_all_machine_states()
    
    async def _initialize_all_machine_detectors(self):
        """Initialize detectors for all machines in the database"""
        try:
            # Get all machines from database
            result = await self.db.execute(select(Machine))
            machines = result.scalars().all()
            
            for machine in machines:
                # Initialize detector using global registry
                get_machine_detector(machine.id)
                    
        except Exception as e:
            logger.error(f"Error initializing machine detectors: {e}")
    
    async def get_machine_thresholds(self, machine_id: str) -> Optional[MachineStateThresholds]:
        """Get machine-specific thresholds from database"""
        result = await self.db.execute(
            select(MachineStateThresholds).where(
                and_(
                    MachineStateThresholds.machine_id == machine_id,
                    MachineStateThresholds.is_active == True
                )
            )
        )
        return result.scalar_one_or_none()
    
    async def create_machine_thresholds(
        self, 
        machine_id: str, 
        thresholds: ThresholdsSchema,
        created_by: Optional[str] = None
    ) -> MachineStateThresholds:
        """Create or update machine thresholds"""
        # Check if thresholds already exist
        existing = await self.get_machine_thresholds(machine_id)
        
        if existing:
            # Update existing
            for field, value in thresholds.dict(exclude_unset=True).items():
                if hasattr(existing, field):
                    setattr(existing, field, value)
            existing.updated_at = datetime.utcnow()
            await self.db.commit()
            await self.db.refresh(existing)
            
            # Reinitialize detector with new thresholds
            remove_machine_detector(machine_id)
            await self.initialize_machine_detector(machine_id)
            
            return existing
        else:
            # Create new
            db_thresholds = MachineStateThresholds(
                machine_id=machine_id,
                **thresholds.dict(),
                created_by=created_by
            )
            self.db.add(db_thresholds)
            await self.db.commit()
            await self.db.refresh(db_thresholds)
            
            # Reinitialize detector with new thresholds
            remove_machine_detector(machine_id)
            await self.initialize_machine_detector(machine_id)
            
            return db_thresholds
    
    async def get_state_history(
        self, 
        machine_id: str, 
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[TransitionSchema]:
        """Get state transition history for a machine"""
        query = select(MachineStateTransition).where(
            MachineStateTransition.machine_id == machine_id
        )
        
        if start_time:
            query = query.where(MachineStateTransition.transition_time >= start_time)
        if end_time:
            query = query.where(MachineStateTransition.transition_time <= end_time)
        
        query = query.order_by(desc(MachineStateTransition.transition_time)).limit(limit)
        
        result = await self.db.execute(query)
        transitions = result.scalars().all()
        
        return [TransitionSchema.from_orm(t) for t in transitions]
    
    async def get_state_statistics(
        self, 
        machine_id: str,
        start_time: datetime,
        end_time: datetime
    ) -> MachineStateStatistics:
        """Calculate state statistics for a time period"""
        # Get state transitions in the period
        result = await self.db.execute(
            select(MachineStateTransition).where(
                and_(
                    MachineStateTransition.machine_id == machine_id,
                    MachineStateTransition.transition_time >= start_time,
                    MachineStateTransition.transition_time <= end_time
                )
            ).order_by(MachineStateTransition.transition_time)
        )
        transitions = result.scalars().all()
        
        # Calculate time spent in each state
        total_seconds = (end_time - start_time).total_seconds()
        state_times = {
            'OFF': 0.0,
            'HEATING': 0.0,
            'IDLE': 0.0,
            'PRODUCTION': 0.0,
            'COOLING': 0.0
        }
        
        # Process transitions to calculate state durations
        for i, transition in enumerate(transitions):
            state = transition.to_state
            start = max(transition.transition_time, start_time)
            
            # Find next transition or end of period
            if i + 1 < len(transitions):
                end = min(transitions[i + 1].transition_time, end_time)
            else:
                end = end_time
            
            duration = (end - start).total_seconds()
            if state in state_times:
                state_times[state] += duration
        
        # Calculate percentages
        state_percentages = {
            state: (time / total_seconds * 100) if total_seconds > 0 else 0
            for state, time in state_times.items()
        }
        
        # Calculate production-specific metrics
        production_transitions = [t for t in transitions if t.to_state == 'PRODUCTION']
        production_cycles = len(production_transitions)
        
        production_durations = []
        for i, transition in enumerate(transitions):
            if transition.to_state == 'PRODUCTION':
                start = transition.transition_time
                # Find when production ended
                end_time_found = end_time
                for j in range(i + 1, len(transitions)):
                    if transitions[j].to_state != 'PRODUCTION':
                        end_time_found = transitions[j].transition_time
                        break
                duration = (end_time_found - start).total_seconds()
                production_durations.append(duration)
        
        avg_production_duration = sum(production_durations) / len(production_durations) if production_durations else 0
        total_production_time = sum(production_durations)
        
        return MachineStateStatistics(
            machine_id=machine_id,
            start_time=start_time,
            end_time=end_time,
            off_percentage=state_percentages['OFF'],
            heating_percentage=state_percentages['HEATING'],
            idle_percentage=state_percentages['IDLE'],
            production_percentage=state_percentages['PRODUCTION'],
            cooling_percentage=state_percentages['COOLING'],
            unknown_percentage=0.0,  # Removed UNKNOWN state
            off_hours=state_times['OFF'] / 3600,
            heating_hours=state_times['HEATING'] / 3600,
            idle_hours=state_times['IDLE'] / 3600,
            production_hours=state_times['PRODUCTION'] / 3600,
            cooling_hours=state_times['COOLING'] / 3600,
            unknown_hours=0.0,  # Removed UNKNOWN state
            total_transitions=len(transitions),
            state_changes=[],
            production_cycles=production_cycles,
            avg_production_duration=avg_production_duration,
            total_production_time=total_production_time
        )
    
    async def _store_machine_state(
        self, 
        machine_id: str, 
        state_info: MachineStateInfo, 
        reading: SensorReading
    ):
        """Store machine state in database"""
        try:
            db_state = MachineState(
                machine_id=machine_id,
                state=state_info.state.value,
                confidence=state_info.confidence,
                state_since=state_info.state_since,
                last_updated=state_info.last_updated,
                temp_avg=state_info.metrics.temp_avg,
                temp_spread=state_info.metrics.temp_spread,
                d_temp_avg=state_info.metrics.d_temp_avg,
                rpm_stable=state_info.metrics.rpm_stable,
                pressure_stable=state_info.metrics.pressure_stable,
                any_temp_above_min=state_info.metrics.any_temp_above_min,
                all_temps_below=state_info.metrics.all_temps_below,
                flags=state_info.flags,
                state_metadata={
                    'sensor_reading': {
                        'screw_rpm': reading.screw_rpm,
                        'pressure_bar': reading.pressure_bar,
                        'temp_zone_1': reading.temp_zone_1,
                        'temp_zone_2': reading.temp_zone_2,
                        'temp_zone_3': reading.temp_zone_3,
                        'temp_zone_4': reading.temp_zone_4,
                        'motor_load': reading.motor_load,
                        'throughput_kg_h': reading.throughput_kg_h
                    }
                }
            )
            self.db.add(db_state)
            await self.db.commit()
        except Exception as e:
            logger.error(f"Error storing machine state for {machine_id}: {e}")
            await self.db.rollback()
    
    async def _log_state_transition(
        self,
        machine_id: str,
        from_state: MachineStateEnum,
        to_state: MachineStateEnum,
        previous_info: MachineStateInfo,
        current_info: MachineStateInfo,
        reading: SensorReading
    ):
        """Log state transition to database"""
        try:
            transition = MachineStateTransition(
                machine_id=machine_id,
                from_state=from_state.value if from_state else None,
                to_state=to_state.value,
                transition_time=datetime.utcnow(),
                previous_state_duration=(
                    (current_info.last_updated - previous_info.state_since).total_seconds()
                    if previous_info.state_since else 0
                ),
                confidence_before=previous_info.confidence,
                confidence_after=current_info.confidence,
                sensor_data={
                    'screw_rpm': reading.screw_rpm,
                    'pressure_bar': reading.pressure_bar,
                    'temp_avg': current_info.metrics.temp_avg,
                    'd_temp_avg': current_info.metrics.d_temp_avg
                },
                transition_metadata={
                    'transition_reason': self._get_transition_reason(from_state, to_state, current_info)
                }
            )
            self.db.add(transition)
            await self.db.commit()
            
            logger.info(f"Logged state transition for {machine_id}: {from_state} → {to_state}")
            
        except Exception as e:
            logger.error(f"Error logging state transition for {machine_id}: {e}")
            await self.db.rollback()
    
    def _get_transition_reason(
        self, 
        from_state: MachineStateEnum, 
        to_state: MachineStateEnum, 
        state_info: MachineStateInfo
    ) -> str:
        """Generate human-readable transition reason"""
        reasons = {
            (MachineStateEnum.OFF, MachineStateEnum.HEATING): "Temperature started rising",
            (MachineStateEnum.HEATING, MachineStateEnum.PRODUCTION): "Production criteria met",
            (MachineStateEnum.IDLE, MachineStateEnum.PRODUCTION): "Production started",
            (MachineStateEnum.PRODUCTION, MachineStateEnum.IDLE): "Production stopped",
            (MachineStateEnum.PRODUCTION, MachineStateEnum.COOLING): "Production stopped, cooling down",
            (MachineStateEnum.COOLING, MachineStateEnum.OFF): "Machine cooled down",
            (MachineStateEnum.HEATING, MachineStateEnum.IDLE): "Temperature stabilized",
            (MachineStateEnum.IDLE, MachineStateEnum.COOLING): "Temperature started falling"
        }
        
        return reasons.get((from_state, to_state), f"State changed to {to_state.value}")
    
    async def _handle_state_actions(
        self, 
        machine_id: str, 
        from_state: MachineStateEnum, 
        to_state: MachineStateEnum
    ):
        """Handle actions based on state changes - automatically sends email notifications"""
        try:
            # Get machine name for email
            machine_name = machine_id
            try:
                result = await self.db.execute(
                    select(Machine).where(Machine.id == machine_id)
                )
                machine = result.scalar_one_or_none()
                if machine:
                    machine_name = machine.name or machine_id
            except Exception:
                pass  # Use machine_id if we can't get name
            
            # Automatically send email notification for state change (non-blocking)
            # This happens automatically whenever machine state changes - no manual trigger needed
            await self._send_state_change_email(
                machine_id, machine_name, from_state, to_state
            )
            
            # Create alerts for important transitions
            # Note: SENSOR_FAULT state removed - sensor faults now default to OFF
            if to_state == MachineStateEnum.PRODUCTION:
                await self._create_alert(
                    machine_id, "PRODUCTION_START", "info",
                    "Production Started",
                    f"Machine {machine_id} has entered production state.",
                    to_state, from_state
                )
            elif from_state == MachineStateEnum.PRODUCTION and to_state != MachineStateEnum.PRODUCTION:
                await self._create_alert(
                    machine_id, "PRODUCTION_END", "info",
                    "Production Ended",
                    f"Machine {machine_id} has exited production state.",
                    to_state, from_state
                )
            
        except Exception as e:
            logger.error(f"Error handling state actions for {machine_id}: {e}")
    
    async def _create_alert(
        self,
        machine_id: str,
        alert_type: str,
        severity: str,
        title: str,
        message: str,
        state: Optional[MachineStateEnum] = None,
        previous_state: Optional[MachineStateEnum] = None
    ):
        """Create machine state alert"""
        try:
            alert = MachineStateAlert(
                machine_id=machine_id,
                alert_type=alert_type,
                severity=severity,
                title=title,
                message=message,
                state=state.value if state else None,
                previous_state=previous_state.value if previous_state else None,
                alert_time=datetime.utcnow()
            )
            self.db.add(alert)
            await self.db.commit()
            
            logger.info(f"Created alert for {machine_id}: {title}")
            
        except Exception as e:
            logger.error(f"Error creating alert for {machine_id}: {e}")
            await self.db.rollback()
    
    async def _send_state_change_email(
        self,
        machine_id: str,
        machine_name: str,
        from_state: MachineStateEnum,
        to_state: MachineStateEnum
    ):
        """Send email notification when machine state changes (fire-and-forget, non-blocking)"""
        try:
            from app.services import notification_service
            from datetime import datetime
            import asyncio
            
            # Only send email if notification service is configured
            if not notification_service.email_configured():
                logger.debug("Email not configured, skipping state change notification")
                return
            
            # Format state names for email
            from_state_name = from_state.value if from_state else "Unknown"
            to_state_name = to_state.value

            # Map states to German+English labels for nicer emails
            state_labels = {
                "OFF": "AUS (OFF)",
                "HEATING": "AUFHEIZEN (HEATING)",
                "IDLE": "BEREIT (IDLE)",
                "PRODUCTION": "PRODUKTION (PRODUCTION)",
                "COOLING": "ABKÜHLEN (COOLING)",
            }
            from_label = state_labels.get(from_state_name, from_state_name)
            to_label = state_labels.get(to_state_name, to_state_name)

            # Helper for a readable German-style timestamp
            now_utc = datetime.utcnow()
            month_names = [
                "Januar", "Februar", "März", "April", "Mai", "Juni",
                "Juli", "August", "September", "Oktober", "November", "Dezember"
            ]
            month_name = month_names[now_utc.month - 1]
            pretty_ts = f"{now_utc.day:02d}. {month_name} {now_utc.year} – {now_utc.hour:02d}:{now_utc.minute:02d} UTC"

            # Use a special template for transition into PRODUCTION, otherwise generic template
            if to_state == MachineStateEnum.PRODUCTION and from_state != MachineStateEnum.PRODUCTION:
                subject = f"Maschinenstatus – Produktionsstart – {machine_name}"
                body = f"""Maschinenstatus – Produktionsstart

Maschine: {machine_name}

Statusänderung
Die Maschine hat den Produktionsbetrieb gestartet.

Vorheriger Zustand: {from_label}
Aktueller Zustand: {to_label}

Zeitpunkt der Änderung:
{pretty_ts}

Bedeutung
Das System hat anhand von Schneckendrehzahl, Druck und Temperatur erkannt, dass die Anlage in den aktiven Produktionsbetrieb gewechselt ist.
Die Produktionsüberwachung und Prozessbewertung sind jetzt aktiv.

Sie können nun die aktuellen Produktionsdaten sowie die Prozessstabilität im Dashboard einsehen.

Dashboard-Zugriff:
http://100.119.197.81:3000/

Diese Benachrichtigung wurde automatisch von der Predictive-Maintenance-Plattform generiert.
"""
            else:
                subject = f"Maschinenstatus – Zustandsänderung – {machine_name}"
                body = f"""Maschinenstatus – Zustandsänderung

Maschine: {machine_name}

Statusänderung
Die Maschine hat ihren Zustand geändert.

Vorheriger Zustand: {from_label}
Aktueller Zustand: {to_label}

Zeitpunkt der Änderung:
{pretty_ts}

Bedeutung
Das System hat anhand der Prozessdaten (z. B. Schneckendrehzahl, Druck und Temperatur) eine Zustandsänderung der Anlage erkannt.

Sie können die aktuellen Zustände und Prozessdaten im Dashboard einsehen.

Dashboard-Zugriff:
http://100.119.197.81:3000/

Diese Benachrichtigung wurde automatisch von der Predictive-Maintenance-Plattform generiert.
"""
            
            # Send email asynchronously (fire-and-forget) so it doesn't block state detection
            # This ensures state detection continues even if email sending is slow or fails
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.get_event_loop()
            
            # Create background task for email sending with proper error handling
            async def send_email_task():
                try:
                    # Get active recipients first to log them
                    from app.services.notification_service import get_active_email_recipients
                    from app.db.session import AsyncSessionLocal
                    
                    async with AsyncSessionLocal() as session:
                        recipients = await get_active_email_recipients(session)
                    
                    if not recipients:
                        logger.warning(f"No active email recipients found for state change notification for {machine_id}")
                        # Fallback to default notification email
                        await notification_service._send_email(
                            subject=subject,
                            body=body,
                            to_override=None,  # Will use fallback from settings
                            use_recipients=False  # Use fallback email
                        )
                        logger.info(f"✅ State change email sent to fallback email for {machine_id}: {from_state_name} → {to_state_name}")
                    else:
                        logger.info(f"Sending state change email to {len(recipients)} recipient(s): {', '.join(recipients)}")
                        await notification_service._send_email(
                            subject=subject,
                            body=body,
                            to_override=None,  # Send to all active recipients
                            use_recipients=True
                        )
                        logger.info(f"✅ State change email sent automatically for {machine_id}: {from_state_name} → {to_state_name}")
                except Exception as email_exc:
                    logger.error(f"⚠️ Failed to send state change email for {machine_id}: {email_exc}", exc_info=True)
            
            # Schedule email sending as background task (non-blocking)
            task = loop.create_task(send_email_task())
            # Add error callback to catch any unhandled exceptions
            def task_done_callback(task):
                try:
                    task.result()  # This will raise if task failed
                except Exception as e:
                    logger.error(f"State change email task failed for {machine_id}: {e}", exc_info=True)
            
            task.add_done_callback(task_done_callback)
            logger.debug(f"State change email task scheduled for {machine_id}: {from_state_name} → {to_state_name}")
                
        except Exception as e:
            logger.error(f"Error scheduling state change email for {machine_id}: {e}", exc_info=True)
    
    async def cleanup_detector(self, machine_id: str):
        """Clean up detector for machine"""
        if machine_id in self._detectors:
            remove_machine_detector(machine_id)
            del self._detectors[machine_id]
