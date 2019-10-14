use eui48_04::MacAddress;

use crate::types::test_type;

#[test]
fn test_eui48_params() {
    test_type(
        "MACADDR",
        &[
            (
                Some(MacAddress::parse_str("12-34-56-AB-CD-EF").unwrap()),
                "'12-34-56-ab-cd-ef'",
            ),
            (None, "NULL"),
        ],
    )
}
