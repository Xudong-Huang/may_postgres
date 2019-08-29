use fallible_iterator::FallibleIterator;
use geo_types_04::{Coordinate, LineString, Point, Rect};
use postgres_protocol::types;
use std::error::Error;

use crate::types::{FromSql, IsNull, ToSql, Type};

impl<'a> FromSql<'a> for Point<f64> {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let point = types::point_from_sql(raw)?;
        Ok(Point::new(point.x(), point.y()))
    }

    accepts!(POINT);
}

impl ToSql for Point<f64> {
    fn to_sql(&self, _: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::point_to_sql(self.x(), self.y(), out);
        Ok(IsNull::No)
    }

    accepts!(POINT);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for Rect<f64> {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let rect = types::box_from_sql(raw)?;
        Ok(Rect {
            min: Coordinate {
                x: rect.lower_left().x(),
                y: rect.lower_left().y(),
            },
            max: Coordinate {
                x: rect.upper_right().x(),
                y: rect.upper_right().y(),
            },
        })
    }

    accepts!(BOX);
}

impl ToSql for Rect<f64> {
    fn to_sql(&self, _: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::box_to_sql(self.min.x, self.min.y, self.max.x, self.max.y, out);
        Ok(IsNull::No)
    }

    accepts!(BOX);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for LineString<f64> {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let path = types::path_from_sql(raw)?;
        let points = path
            .points()
            .map(|p| Ok(Coordinate { x: p.x(), y: p.y() }))
            .collect()?;
        Ok(LineString(points))
    }

    accepts!(PATH);
}

impl ToSql for LineString<f64> {
    fn to_sql(&self, _: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let closed = false; // always encode an open path from LineString
        types::path_to_sql(closed, self.0.iter().map(|p| (p.x, p.y)), out)?;
        Ok(IsNull::No)
    }

    accepts!(PATH);
    to_sql_checked!();
}
