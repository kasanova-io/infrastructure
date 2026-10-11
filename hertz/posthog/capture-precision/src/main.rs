fn main() {
    // Synthetic values only. No source event or user data enters this check.
    let mut mismatches = 0usize;
    let mut cases = 0usize;
    for divisor in [1013.0, 9973.0, 10000019.0] {
        for numerator in 1..=20000 {
            let original = numerator as f64 / divisor;
            let encoded = serde_json::to_string(&original).unwrap();
            let decoded: f64 = serde_json::from_str(&encoded).unwrap();
            cases += 1;
            if original.to_bits() != decoded.to_bits() {
                mismatches += 1;
            }
        }
    }
    println!("{}", serde_json::json!({
        "synthetic_cases": cases,
        "parser_version": "1.0.149",
        "float_roundtrip_enabled": cfg!(feature = "precise"),
        "binary_value_mismatches": mismatches
    }));
    if cfg!(feature = "precise") {
        assert_eq!(mismatches, 0, "Precise parser changed a numeric value");
    } else {
        assert!(mismatches > 0, "Default parser drift was not reproduced");
    }
}
