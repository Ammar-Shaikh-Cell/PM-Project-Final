import csv
from datetime import datetime
from pathlib import Path
from typing import List, Sequence
import tempfile

from fpdf import FPDF
from loguru import logger
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.models.sensor_data import SensorData
from app.schemas.report import ReportRequest, ReportResponse
from typing import Any

settings = get_settings()
settings.reports_dir.mkdir(parents=True, exist_ok=True)


async def _fetch_rows(session: AsyncSession, params: ReportRequest) -> Sequence[SensorData]:
    # Only select columns that exist in the database (exclude readings and raw_payload if they don't exist)
    stmt = select(
        SensorData.id,
        SensorData.sensor_id,
        SensorData.machine_id,
        SensorData.timestamp,
        SensorData.value,
        SensorData.status,
        SensorData.metadata_json,
        SensorData.idempotency_key,
        SensorData.created_at,
        SensorData.updated_at
    )
    filters = []
    if params.machine_id:
        filters.append(SensorData.machine_id == params.machine_id)
    if params.sensor_id:
        filters.append(SensorData.sensor_id == params.sensor_id)
    if params.date_from:
        filters.append(SensorData.timestamp >= params.date_from)
    if params.date_to:
        filters.append(SensorData.timestamp <= params.date_to)
    if filters:
        stmt = stmt.where(and_(*filters))
    # Optimize: Order by timestamp desc and limit to 5000 rows max for faster CSV generation
    stmt = stmt.order_by(SensorData.timestamp.desc()).limit(5000)
    result = await session.execute(stmt)
    # Convert to SensorData-like objects
    rows = []
    for row in result.all():
        # Create a simple object with the attributes we need
        class SensorDataRow:
            def __init__(self, row_data):
                self.id = row_data.id
                self.sensor_id = row_data.sensor_id
                self.machine_id = row_data.machine_id
                self.timestamp = row_data.timestamp
                self.value = row_data.value
                self.status = row_data.status
                self.metadata = row_data.metadata_json
        rows.append(SensorDataRow(row))
    # Reverse to chronological order for CSV
    return list(reversed(rows))


def _write_csv(rows: Sequence[SensorData], path: Path) -> None:
    fieldnames = ["timestamp", "machine_id", "sensor_id", "value", "status"]
    try:
        # Use buffered writing for better performance
        with path.open("w", newline="", encoding="utf-8", buffering=8192) as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            # Batch write for better performance
            batch_size = 100
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                for row in batch:
                    try:
                        writer.writerow(
                            {
                                "timestamp": row.timestamp.isoformat() if row.timestamp else "",
                                "machine_id": str(row.machine_id) if row.machine_id else "",
                                "sensor_id": str(row.sensor_id) if row.sensor_id else "",
                                "value": float(row.value) if row.value is not None else 0.0,
                                "status": str(row.status) if row.status else "unknown",
                            }
                        )
                    except Exception as e:
                        logger.warning("Error writing CSV row: {}", e)
                        continue
        logger.info("CSV file written successfully: {} ({} rows)", path, len(rows))
    except Exception as e:
        logger.error("Error writing CSV file: {}", e, exc_info=True)
        raise


def _write_pdf(rows: Sequence[SensorData], path: Path) -> None:
    """
    Generate a PDF report that includes charts.

    We primarily target the current extruder/MSSQL ingestion shape where `sensor_data.metadata_json`
    contains keys like: screw_rpm, pressure_bar, temp_zone1_c..temp_zone4_c.
    If those fields are not present, we fallback to plotting the generic `value` series.
    """
    # Matplotlib is used purely server-side (Agg backend) to render PNGs embedded into the PDF.
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    def _to_float(v: Any) -> float | None:
        if v is None:
            return None
        try:
            if isinstance(v, bool):
                return None
            return float(v)
        except Exception:
            return None

    def _meta(row: Any) -> dict:
        m = getattr(row, "metadata", None)
        return m if isinstance(m, dict) else {}

    # Build timeseries
    xs: List[datetime] = []
    rpm: List[float] = []
    pressure: List[float] = []
    tz1: List[float] = []
    tz2: List[float] = []
    tz3: List[float] = []
    tz4: List[float] = []
    generic_value: List[float] = []

    for r in rows:
        ts = getattr(r, "timestamp", None)
        if not isinstance(ts, datetime):
            continue
        m = _meta(r)

        xs.append(ts)
        generic_value.append(_to_float(getattr(r, "value", None)) or 0.0)

        rpm.append(_to_float(m.get("screw_rpm")) or 0.0)
        pressure.append(_to_float(m.get("pressure_bar")) or 0.0)
        tz1.append(_to_float(m.get("temp_zone1_c")) or 0.0)
        tz2.append(_to_float(m.get("temp_zone2_c")) or 0.0)
        tz3.append(_to_float(m.get("temp_zone3_c")) or 0.0)
        tz4.append(_to_float(m.get("temp_zone4_c")) or 0.0)

    def _has_extruder_metadata() -> bool:
        if not xs:
            return False
        # If most of the series are all zeros, treat as missing metadata.
        def nonzero_ratio(series: List[float]) -> float:
            if not series:
                return 0.0
            nz = sum(1 for v in series if abs(v) > 1e-9)
            return nz / max(1, len(series))
        return nonzero_ratio(pressure) > 0.05 or nonzero_ratio(rpm) > 0.05 or nonzero_ratio(tz1) > 0.05

    def _plot_line(x: List[datetime], y: List[float], *, title: str, ylabel: str, out_path: Path) -> None:
        plt.figure(figsize=(10.5, 3.2), dpi=140)
        plt.plot(x, y, linewidth=1.6)
        plt.title(title)
        plt.xlabel("Time")
        plt.ylabel(ylabel)
        plt.grid(True, alpha=0.25)
        plt.tight_layout()
        plt.savefig(out_path, format="png")
        plt.close()

    def _plot_temps(x: List[datetime], t1: List[float], t2: List[float], t3: List[float], t4: List[float], *, out_path: Path) -> None:
        plt.figure(figsize=(10.5, 3.2), dpi=140)
        plt.plot(x, t1, label="Zone 1", linewidth=1.3)
        plt.plot(x, t2, label="Zone 2", linewidth=1.3)
        plt.plot(x, t3, label="Zone 3", linewidth=1.3)
        plt.plot(x, t4, label="Zone 4", linewidth=1.3)
        plt.title("Temperature Zones (°C)")
        plt.xlabel("Time")
        plt.ylabel("°C")
        plt.grid(True, alpha=0.25)
        plt.legend(loc="upper right", fontsize=8)
        plt.tight_layout()
        plt.savefig(out_path, format="png")
        plt.close()

    def _temp_avg_spread(t1: List[float], t2: List[float], t3: List[float], t4: List[float]) -> tuple[List[float], List[float]]:
        avg: List[float] = []
        spread: List[float] = []
        for a, b, c, d in zip(t1, t2, t3, t4):
            vals = [a, b, c, d]
            avg.append(sum(vals) / 4.0)
            spread.append(max(vals) - min(vals))
        return avg, spread

    pdf = FPDF(unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=12)

    # Title page
    pdf.add_page()
    pdf.set_font("Arial", "B", 16)
    pdf.cell(0, 10, "Predictive Maintenance Report (PDF)", ln=True)
    pdf.set_font("Arial", size=11)
    if rows:
        start_ts = getattr(rows[0], "timestamp", None)
        end_ts = getattr(rows[-1], "timestamp", None)
        pdf.cell(0, 8, f"Period: {start_ts.isoformat() if start_ts else '—'}  to  {end_ts.isoformat() if end_ts else '—'}", ln=True)
    pdf.cell(0, 8, f"Data points: {len(rows)}", ln=True)
    pdf.ln(2)
    pdf.set_font("Arial", size=10)
    pdf.multi_cell(
        0,
        6,
        "This report contains time-series charts derived from ingested sensor data. "
        "If extruder MSSQL metadata is available, charts show screw speed, pressure, and temperature zones; "
        "otherwise a generic sensor value chart is included.",
    )

    if not xs:
        pdf.ln(4)
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 8, "No data available for the selected criteria.", ln=True)
        pdf.output(str(path))
        return

    # Render charts to temporary files and embed
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        if _has_extruder_metadata():
            p_pressure = tmpdir_path / "pressure.png"
            p_rpm = tmpdir_path / "rpm.png"
            p_temps = tmpdir_path / "temps.png"
            p_temp_spread = tmpdir_path / "temp_spread.png"

            _plot_line(xs, pressure, title="Pressure (bar)", ylabel="bar", out_path=p_pressure)
            _plot_line(xs, rpm, title="Screw Speed (rpm)", ylabel="rpm", out_path=p_rpm)
            _plot_temps(xs, tz1, tz2, tz3, tz4, out_path=p_temps)

            temp_avg, temp_spread = _temp_avg_spread(tz1, tz2, tz3, tz4)
            _plot_line(xs, temp_spread, title="Temperature Spread (°C)", ylabel="°C", out_path=p_temp_spread)

            for title, img in [
                ("Pressure", p_pressure),
                ("Screw Speed", p_rpm),
                ("Temperature Zones", p_temps),
                ("Temperature Spread", p_temp_spread),
            ]:
                pdf.add_page()
                pdf.set_font("Arial", "B", 13)
                pdf.cell(0, 8, title, ln=True)
                pdf.ln(2)
                # Fit image to page width (A4 minus margins ~190mm)
                pdf.image(str(img), x=10, w=190)
        else:
            p_generic = tmpdir_path / "value.png"
            _plot_line(xs, generic_value, title="Sensor Value", ylabel="value", out_path=p_generic)
            pdf.add_page()
            pdf.set_font("Arial", "B", 13)
            pdf.cell(0, 8, "Sensor Value", ln=True)
            pdf.ln(2)
            pdf.image(str(p_generic), x=10, w=190)

    pdf.output(str(path))


async def generate_report_fast(session: AsyncSession, params: ReportRequest) -> ReportResponse:
    """Fast CSV generation optimized for speed"""
    try:
        rows = await _fetch_rows(session, params)
        logger.info("Fetched {} rows for fast CSV report", len(rows))
        
        if not rows:
            raise ValueError("No data found for the selected criteria")
        
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        file_name = f"report_{timestamp}.csv"
        report_path = settings.reports_dir / file_name

        # Ensure reports directory exists
        settings.reports_dir.mkdir(parents=True, exist_ok=True)

        # Fast CSV writing
        _write_csv(rows, report_path)

        logger.info("Fast CSV report generated at {}", report_path)
        
        if not report_path.exists():
            raise FileNotFoundError(f"Report file was not created at {report_path}")
        
        return ReportResponse(
            report_name=file_name, 
            url=f"/reports/download/{file_name}", 
            generated_at=datetime.utcnow()
        )
    except Exception as e:
        logger.error("Error generating fast CSV report: {}", e, exc_info=True)
        raise


async def generate_report(session: AsyncSession, params: ReportRequest) -> ReportResponse:
    try:
        if params.format != "pdf":
            raise ValueError("Only PDF reports are supported")

        rows = await _fetch_rows(session, params)
        logger.info("Fetched {} rows for report", len(rows))
        
        if not rows:
            logger.warning("No data found for report generation")
            # Return empty report or raise error
            raise ValueError("No data found for the selected criteria")
        
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        file_name = f"report_{timestamp}.{params.format}"
        report_path = settings.reports_dir / file_name

        # Ensure reports directory exists
        settings.reports_dir.mkdir(parents=True, exist_ok=True)

        _write_pdf(rows, report_path)

        logger.info("Report generated at {}", report_path)
        
        # Verify file was created
        if not report_path.exists():
            raise FileNotFoundError(f"Report file was not created at {report_path}")
        
        return ReportResponse(
            report_name=file_name, 
            url=f"/reports/download/{file_name}", 
            generated_at=datetime.utcnow()
        )
    except Exception as e:
        logger.error("Error generating report: {}", e, exc_info=True)
        raise

