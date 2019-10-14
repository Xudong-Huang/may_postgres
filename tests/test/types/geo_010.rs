use geo_010::{Coordinate, LineString, Point, Rect};

use crate::types::test_type;

#[test]
fn test_point_params() {
    test_type(
        "POINT",
        &[
            (Some(Point::new(0.0, 0.0)), "POINT(0, 0)"),
            (Some(Point::new(-3.14, 1.618)), "POINT(-3.14, 1.618)"),
            (None, "NULL"),
        ],
    );
}

#[test]
fn test_box_params() {
    test_type(
        "BOX",
        &[
            (
                Some(Rect {
                    min: Coordinate { x: -3.14, y: 1.618 },
                    max: Coordinate {
                        x: 160.0,
                        y: 69701.5615,
                    },
                }),
                "BOX(POINT(160.0, 69701.5615), POINT(-3.14, 1.618))",
            ),
            (None, "NULL"),
        ],
    );
}

#[test]
fn test_path_params() {
    let points = vec![
        Coordinate { x: 0., y: 0. },
        Coordinate { x: -3.14, y: 1.618 },
        Coordinate {
            x: 160.0,
            y: 69701.5615,
        },
    ];
    test_type(
        "PATH",
        &[
            (
                Some(LineString(points)),
                "path '((0, 0), (-3.14, 1.618), (160.0, 69701.5615))'",
            ),
            (None, "NULL"),
        ],
    );
}
