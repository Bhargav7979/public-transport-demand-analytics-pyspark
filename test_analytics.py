"""
Unit and Integration Tests for Public Transport Passenger Demand Analytics
"""

import os
import unittest
import pandas as pd
import analysis


class TestTransportAnalytics(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sample_path = os.path.join(os.path.dirname(__file__), "data", "passenger_data.csv")
        assert os.path.exists(cls.sample_path), f"Sample data not found at {cls.sample_path}"
        cls.df = pd.read_csv(cls.sample_path)

    def test_schema_validation_valid_data(self):
        """Test that the bundled synthetic dataset passes schema validation."""
        is_valid, errors, summary = analysis.validate_dataset_schema(self.df)
        self.assertTrue(is_valid, f"Expected valid dataset, got errors: {errors}")
        self.assertEqual(len(errors), 0)
        self.assertEqual(summary["total_rows"], 1080)
        self.assertEqual(len(summary["missing_columns"]), 0)

    def test_schema_validation_missing_columns(self):
        """Test that missing required columns are properly detected and reported."""
        invalid_df = self.df.drop(columns=["BusesOperating", "TimeSlot"])
        is_valid, errors, summary = analysis.validate_dataset_schema(invalid_df)
        self.assertFalse(is_valid)
        self.assertTrue(any("Missing required column" in err for err in errors))
        self.assertIn("BusesOperating", summary["missing_columns"])
        self.assertIn("TimeSlot", summary["missing_columns"])

    def test_schema_validation_non_numeric(self):
        """Test that non-numeric passenger values are caught."""
        bad_df = self.df.copy()
        bad_df["Passengers"] = bad_df["Passengers"].astype(object)
        bad_df.loc[0, "Passengers"] = "INVALID_NUMBER"
        is_valid, errors, summary = analysis.validate_dataset_schema(bad_df)
        self.assertFalse(is_valid)
        self.assertTrue(any("Passengers" in err for err in errors))

    def test_clean_dataset(self):
        """Test that cleaning coerces types and removes empty records."""
        df_dirty = self.df.copy()
        df_dirty.loc[1, "Passengers"] = None
        df_dirty.loc[2, "BusesOperating"] = -5
        cleaned = analysis.clean_dataset(df_dirty)

        self.assertFalse(cleaned.empty)
        self.assertTrue(pd.api.types.is_integer_dtype(cleaned["Passengers"]))
        self.assertTrue(pd.api.types.is_integer_dtype(cleaned["BusesOperating"]))
        # BusesOperating should be at least 1
        self.assertTrue((cleaned["BusesOperating"] >= 1).all())

    def test_analytics_kpi_outputs(self):
        """Test that the analytics pipeline computes required KPIs."""
        results = analysis.analyze_transport_demand(self.df)
        self.assertIn("engine", results)
        self.assertIn("kpis", results)

        kpis = results["kpis"]
        self.assertGreater(kpis["total_passengers"], 0)
        self.assertGreater(kpis["total_buses"], 0)
        self.assertGreater(kpis["total_records"], 0)
        self.assertGreater(kpis["avg_passengers_per_slot"], 0)
        self.assertIsNotNone(kpis["top_route"])
        self.assertIsNotNone(kpis["peak_timeslot"])

    def test_analytics_route_summary(self):
        """Test that route aggregations produce correct columns and ordering."""
        results = analysis.analyze_transport_demand(self.df)
        route_df = results["route_summary"]
        required_cols = {"Route", "TotalPassengers", "AvgPassengersPerSlot", "PeakPassengerLoad", "TotalBusesOperating", "AvgPassengersPerBus"}
        self.assertTrue(required_cols.issubset(route_df.columns))
        # Check sorted descending by TotalPassengers
        self.assertTrue(route_df["TotalPassengers"].is_monotonic_decreasing)

    def test_analytics_timeslot_summary(self):
        """Test that peak time slots are computed and sorted."""
        results = analysis.analyze_transport_demand(self.df)
        ts_df = results["timeslot_summary"]
        required_cols = {"TimeSlot", "TotalPassengers", "AvgPassengers", "MaxSlotPassengers", "TotalBuses"}
        self.assertTrue(required_cols.issubset(ts_df.columns))
        self.assertTrue(ts_df["TotalPassengers"].is_monotonic_decreasing)

    def test_analytics_daily_trends(self):
        """Test that daily trends are computed and sorted by date ascending."""
        results = analysis.analyze_transport_demand(self.df)
        daily_df = results["daily_summary"]
        required_cols = {"Date", "DailyPassengers", "DailyBuses", "AvgSlotPassengers"}
        self.assertTrue(required_cols.issubset(daily_df.columns))
        self.assertTrue(daily_df["Date"].is_monotonic_increasing)

    def test_pivot_matrix(self):
        """Test that Route x TimeSlot pivot matrix is generated."""
        results = analysis.analyze_transport_demand(self.df)
        pivot_df = results["pivot_matrix"]
        self.assertIn("Route", pivot_df.columns)
        self.assertGreater(len(pivot_df.columns), 2)


if __name__ == "__main__":
    unittest.main()
