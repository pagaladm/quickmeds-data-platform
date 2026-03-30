def bronze_validator(df, table_name, key_column, expected_columns):

    results = {}
    print(f"\n[QuickMeds] Bronze Validator — {table_name}")
    print("=" * 55)

    # 1. Row Count
    row_count = df.count()
    results["row_count"] = "pass" if row_count > 0 else "fail"
    print(f"  1. Row Count     : {row_count} rows → {results['row_count'].upper()}")

    # 2. Null Check
    from pyspark.sql.functions import col
    null_count = df.filter(col(key_column).isNull() | (col(key_column) == "")).count()
    null_pct   = (null_count / row_count * 100) if row_count > 0 else 100
    results["null_check"]  = "pass" if null_pct < 5 else "fail"
    results["null_count"]  = null_count
    print(f"  2. Null Check    : {null_count} nulls ({null_pct:.1f}%) → {results['null_check'].upper()}")

    # 3. Schema Check
    missing_cols = [c for c in expected_columns if c not in df.columns]
    results["schema_check"]    = "pass" if not missing_cols else "fail"
    results["missing_columns"] = missing_cols
    print(f"  3. Schema Check  : missing={missing_cols} → {results['schema_check'].upper()}")

    # 4. Metadata Check
    metadata_cols = ["_source", "_ingest_ts", "_file_name", "_run_id"]
    missing_meta  = [c for c in metadata_cols if c not in df.columns]
    results["metadata_check"] = "pass" if not missing_meta else "fail"
    print(f"  4. Metadata Check: missing={missing_meta} → {results['metadata_check'].upper()}")

    print("=" * 55)
    return results