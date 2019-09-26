use bit_vec_07::BitVec;

use crate::types::test_type;

#[test]
fn test_bit_params() {
    let mut bv = BitVec::from_bytes(&[0b0110_1001, 0b0000_0111]);
    bv.pop();
    bv.pop();
    test_type(
        "BIT(14)",
        vec![(Some(bv), "B'01101001000001'"), (None, "NULL")],
    )
}

#[test]
fn test_varbit_params() {
    let mut bv = BitVec::from_bytes(&[0b0110_1001, 0b0000_0111]);
    bv.pop();
    bv.pop();
    test_type(
        "VARBIT",
        vec![
            (Some(bv), "B'01101001000001'"),
            (Some(BitVec::from_bytes(&[])), "B''"),
            (None, "NULL"),
        ],
    )
}
