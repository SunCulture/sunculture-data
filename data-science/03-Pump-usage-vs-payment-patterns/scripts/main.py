from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# Load the data
df = pd.read_csv("files/IoT Data.csv")

print("Data loaded successfully!")
print(f"Shape: {df.shape}")
print("\nColumn names:")
print(df.columns.tolist())
print("\nFirst few rows:")
print(df.head())


def analyze_runtime_counter_method(df):
    """
    Method 1: Analyze pump usage using runtime counter from metadata reports
    This is the most accurate method using device-side calculated runtime
    """

    # First, let's identify the relevant columns
    print("\n" + "=" * 60)
    print("ANALYZING DATA STRUCTURE FOR RUNTIME COUNTER METHOD")
    print("=" * 60)

    # Check for potential runtime counter columns
    runtime_columns = [
        col
        for col in df.columns
        if "runtime" in col.lower() or "counter" in col.lower()
    ]
    device_columns = [
        col for col in df.columns if "device" in col.lower() or "id" in col.lower()
    ]
    timestamp_columns = [
        col for col in df.columns if "time" in col.lower() or "date" in col.lower()
    ]

    print(f"\nPotential runtime counter columns: {runtime_columns}")
    print(f"Potential device ID columns: {device_columns}")
    print(f"Potential timestamp columns: {timestamp_columns}")

    # If no runtime counter columns found, we'll need to work with what we have
    if not runtime_columns:
        print("\n⚠️  WARNING: No runtime counter columns detected!")
        print("This method requires metadata reports with runtime_counter_minutes")
        print(
            "Available columns suggest this might be aggregated telemetry data instead"
        )
        print("\nLet's check what we can analyze with the current data structure...")

        # Check for motor current data which indicates pump activity
        current_columns = [
            col
            for col in df.columns
            if "current" in col.lower() and "motor" in col.lower()
        ]
        if current_columns:
            print(f"\nFound motor current data: {current_columns}")
            return analyze_with_available_data(df)
        else:
            print("No motor current data found either.")
            return None

    # If we found runtime columns, proceed with runtime counter analysis
    return analyze_runtime_counters(
        df, runtime_columns, device_columns, timestamp_columns
    )


def analyze_runtime_counters(df, runtime_columns, device_columns, timestamp_columns):
    """
    Analyze using actual runtime counter data
    """
    # Use the first available columns (adjust as needed based on your data)
    runtime_col = runtime_columns[0]
    device_col = device_columns[0] if device_columns else "DeviceId"
    timestamp_col = timestamp_columns[0] if timestamp_columns else "timestamp"

    print(f"\nUsing columns:")
    print(f"  Runtime Counter: {runtime_col}")
    print(f"  Device ID: {device_col}")
    print(f"  Timestamp: {timestamp_col}")

    # Prepare the data
    analysis_df = df.copy()

    # Convert timestamp to datetime if it isn't already
    if timestamp_col in analysis_df.columns:
        analysis_df[timestamp_col] = pd.to_datetime(analysis_df[timestamp_col])
        analysis_df["date"] = analysis_df[timestamp_col].dt.date
    else:
        print("No timestamp column found - using row-based analysis")
        analysis_df["date"] = pd.to_datetime("today").date()

    # Group by device and date to calculate daily runtime
    if "date" in analysis_df.columns:
        daily_runtime = (
            analysis_df.groupby([device_col, "date"])
            .agg(
                {
                    runtime_col: ["min", "max", "count"],
                    timestamp_col: (
                        "count" if timestamp_col in analysis_df.columns else lambda x: 1
                    ),
                }
            )
            .reset_index()
        )

        # Flatten column names
        daily_runtime.columns = [
            device_col,
            "date",
            "min_counter",
            "max_counter",
            "counter_readings",
            "timestamp_count",
        ]

        # Calculate daily runtime (difference between max and min counter for each day)
        daily_runtime["daily_runtime_minutes"] = (
            daily_runtime["max_counter"] - daily_runtime["min_counter"]
        )
        daily_runtime["daily_runtime_hours"] = (
            daily_runtime["daily_runtime_minutes"] / 60
        )

        print(f"\nDaily Runtime Analysis:")
        print(daily_runtime[daily_runtime["daily_runtime_minutes"] > 0].head(10))

    # Device-level summary
    device_summary = (
        analysis_df.groupby(device_col)
        .agg({runtime_col: ["min", "max", "count"]})
        .reset_index()
    )

    # Flatten column names
    device_summary.columns = [
        device_col,
        "min_counter",
        "max_counter",
        "total_readings",
    ]

    # Calculate total runtime per device
    device_summary["total_runtime_minutes"] = (
        device_summary["max_counter"] - device_summary["min_counter"]
    )
    device_summary["total_runtime_hours"] = device_summary["total_runtime_minutes"] / 60

    # Filter to only devices with actual runtime
    active_devices = device_summary[device_summary["total_runtime_minutes"] > 0].copy()

    return (
        device_summary,
        active_devices,
        daily_runtime if "daily_runtime" in locals() else None,
    )


def analyze_with_available_data(df):
    """
    Fallback analysis using available telemetry data structure
    """
    print("\n" + "=" * 60)
    print("ANALYZING WITH AVAILABLE TELEMETRY DATA")
    print("=" * 60)

    # Look for the actual data structure in your CSV
    expected_cols = [
        "DeviceId",
        "avg_MotorCurrent",
        "total_active_records",
        "num_days_with_data",
    ]

    if all(col in df.columns for col in expected_cols):
        print("✅ Found expected aggregated telemetry structure")

        # Identify active pumps (current > 1A threshold)
        df["pump_active"] = df["avg_MotorCurrent"] > 1.0

        # Estimate runtime based on active records
        # Each active record represents a telemetry point where pump was running
        sampling_interval_minutes = 15  # Adjust based on your telemetry frequency

        df["estimated_runtime_minutes"] = np.where(
            df["pump_active"], df["total_active_records"] * sampling_interval_minutes, 0
        )

        df["estimated_runtime_hours"] = df["estimated_runtime_minutes"] / 60
        df["avg_daily_runtime_hours"] = (
            df["estimated_runtime_hours"] / df["num_days_with_data"]
        )

        # Filter active devices
        active_devices = df[df["pump_active"]].copy()

        # Aggregate properly at device level to avoid impossible daily usage
        device_level_summary = (
            df.groupby("DeviceId")
            .agg(
                {
                    "estimated_runtime_hours": "sum",
                    "num_days_with_data": "sum",
                    "avg_MotorCurrent": "mean",
                    "total_active_records": "sum",
                    "pump_active": "any",  # True if device was active in any month
                }
            )
            .reset_index()
        )

        # Recalculate realistic daily averages
        device_level_summary["avg_daily_runtime_hours"] = (
            device_level_summary["estimated_runtime_hours"]
            / device_level_summary["num_days_with_data"]
        )

        # Filter to only devices that were active in at least one period
        active_devices_corrected = device_level_summary[
            device_level_summary["pump_active"]
        ].copy()

        print("\n📊 PUMP USAGE SUMMARY (Corrected - Device Level):")
        print("-" * 50)

        # Check for any devices still showing >24 hrs/day (data quality issues)
        suspicious_devices = active_devices_corrected[
            active_devices_corrected["avg_daily_runtime_hours"] > 24
        ]
        if len(suspicious_devices) > 0:
            print(
                f"⚠️  WARNING: {len(suspicious_devices)} devices showing >24 hrs/day usage (data quality issue)"
            )
            print("Top suspicious devices:")
            for _, row in suspicious_devices.head(5).iterrows():
                print(
                    f"  Device {row['DeviceId']}: {row['avg_daily_runtime_hours']:.1f} hrs/day "
                    f"({row['estimated_runtime_hours']:.0f} total hrs over {row['num_days_with_data']} days)"
                )

        # Show realistic usage examples
        realistic_devices = active_devices_corrected[
            active_devices_corrected["avg_daily_runtime_hours"] <= 24
        ]
        print(
            f"\n✅ Devices with realistic usage (≤24 hrs/day): {len(realistic_devices):,}"
        )

        if len(realistic_devices) > 0:
            print("Sample realistic usage devices:")
            for _, row in realistic_devices.head(5).iterrows():
                print(
                    f"  Device {row['DeviceId']}: {row['avg_daily_runtime_hours']:.1f} hrs/day "
                    f"({row['estimated_runtime_hours']:.0f} total hrs, {row['avg_MotorCurrent']:.2f}A avg)"
                )

        # Additional detailed analysis for large dataset
        print(f"\n📈 DETAILED ANALYSIS (Corrected):")
        print(f"Total data points: {len(df):,}")
        print(f"Unique devices: {df['DeviceId'].nunique():,}")
        print(f"Active devices (ever >1A): {len(active_devices_corrected):,}")
        print(f"Devices with realistic usage: {len(realistic_devices):,}")
        print(
            f"Devices with suspicious usage (>24hrs/day): {len(suspicious_devices):,}"
        )

        # Time period analysis
        time_summary = (
            df.groupby(["Year", "Month"])
            .agg(
                {
                    "DeviceId": "nunique",
                    "pump_active": "sum",
                    "estimated_runtime_hours": "sum",
                }
            )
            .reset_index()
        )

        print(f"\n📅 TIME PERIOD BREAKDOWN:")
        for _, period in time_summary.iterrows():
            # Convert to int to handle the float formatting issue
            year = int(period["Year"])
            month = int(period["Month"])
            devices = int(period["DeviceId"])
            active_pumps = int(period["pump_active"])
            runtime_hours = period["estimated_runtime_hours"]

            print(
                f"{year}-{month:02d}: {devices} devices, "
                f"{active_pumps} active pumps, {runtime_hours:.1f} total hours"
            )

        # Current distribution analysis
        current_ranges = pd.cut(
            df["avg_MotorCurrent"],
            bins=[0, 0.1, 1.0, 2.0, 5.0, float("inf")],
            labels=[
                "No current (0-0.1A)",
                "Low current (0.1-1A)",
                "Active pumping (1-2A)",
                "High usage (2-5A)",
                "Very high (>5A)",
            ],
        )

        print(f"\n⚡ MOTOR CURRENT DISTRIBUTION:")
        current_dist = current_ranges.value_counts().sort_index()
        for range_name, count in current_dist.items():
            percentage = (count / len(df)) * 100
            print(f"  {range_name}: {count:,} records ({percentage:.1f}%)")

        return df, active_devices_corrected

    else:
        print("❌ Data structure doesn't match expected format")
        print("Available columns:", df.columns.tolist())
        return None


# Run the analysis
print("Starting Runtime Counter Analysis...")
result = analyze_runtime_counter_method(df)

if result:
    if len(result) == 2:  # Available data analysis
        full_data, active_devices = result

        print(f"\n🎯 SUMMARY RESULTS (Corrected):")
        print(f"Total data points: {len(full_data):,}")
        print(f"Active devices (ever >1A): {len(active_devices):,}")
        print(
            f"Total pump usage time: {active_devices['estimated_runtime_hours'].sum():.1f} hours"
        )
        if len(active_devices) > 0:
            realistic_devices = active_devices[
                active_devices["avg_daily_runtime_hours"] <= 24
            ]
            if len(realistic_devices) > 0:
                print(
                    f"Average daily usage (realistic devices): {realistic_devices['avg_daily_runtime_hours'].mean():.1f} hours/day"
                )

            # Show top 10 devices by total usage (corrected)
            print(f"\n🏆 TOP 10 DEVICES BY TOTAL USAGE (Device Level):")
            top_devices = active_devices.nlargest(10, "estimated_runtime_hours")
            for i, (_, row) in enumerate(top_devices.iterrows(), 1):
                status = "⚠️ SUSPICIOUS" if row["avg_daily_runtime_hours"] > 24 else "✅"
                print(
                    f"  {i:2d}. Device {row['DeviceId']}: {row['estimated_runtime_hours']:.1f} hours "
                    f"({row['avg_daily_runtime_hours']:.1f} hrs/day) {status}"
                )
        else:
            print("No active devices found!")

        # Additional insights
        if len(active_devices) > 0:
            realistic_devices = active_devices[
                active_devices["avg_daily_runtime_hours"] <= 24
            ]
            print(f"\n💡 INSIGHTS:")
            print(
                f"  • {len(realistic_devices)} devices with realistic usage (≤24 hrs/day)"
            )
            if len(realistic_devices) > 0:
                print(
                    f"  • Realistic devices average: {realistic_devices['avg_daily_runtime_hours'].mean():.1f} hrs/day"
                )
                print(
                    f"  • Peak realistic usage: {realistic_devices['avg_daily_runtime_hours'].max():.1f} hrs/day"
                )

            suspicious_count = len(
                active_devices[active_devices["avg_daily_runtime_hours"] > 24]
            )
            if suspicious_count > 0:
                print(
                    f"  • {suspicious_count} devices need data quality review (>24 hrs/day)"
                )
                print(
                    f"  • Possible causes: overlapping records, incorrect sampling assumptions"
                )

    elif len(result) == 3:  # Runtime counter analysis
        device_summary, active_devices, daily_runtime = result

        print(f"\n🎯 RUNTIME COUNTER RESULTS:")
        print(f"Total devices: {len(device_summary)}")
        print(f"Active devices: {len(active_devices)}")

        if len(active_devices) > 0:
            print(f"\nTop devices by runtime:")
            top_devices = active_devices.nlargest(5, "total_runtime_hours")
            for _, row in top_devices.iterrows():
                print(
                    f"  {row[device_summary.columns[0]]}: {row['total_runtime_hours']:.1f} hours"
                )

            print(
                f"\nTotal system runtime: {active_devices['total_runtime_hours'].sum():.1f} hours"
            )

else:
    print("❌ Could not analyze data with current structure")

print(f"\n📋 DATA VALIDATION:")
print(f"  Rows processed: {len(df)}")
print(f"  Columns available: {len(df.columns)}")
print(f"  Data types: {df.dtypes.value_counts().to_dict()}")

# Show sample of original data for reference
print(f"\n📄 SAMPLE OF ORIGINAL DATA (first 5 rows):")
print(df.head())
