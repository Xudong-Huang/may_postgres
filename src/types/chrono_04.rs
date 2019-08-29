use chrono_04::{DateTime, Duration, FixedOffset, Local, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use postgres_protocol::types;
use std::error::Error;

use crate::types::{FromSql, IsNull, ToSql, Type};

fn base() -> NaiveDateTime {
    NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0)
}

impl<'a> FromSql<'a> for NaiveDateTime {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<NaiveDateTime, Box<dyn Error + Sync + Send>> {
        let t = types::timestamp_from_sql(raw)?;
        Ok(base() + Duration::microseconds(t))
    }

    accepts!(TIMESTAMP);
}

impl ToSql for NaiveDateTime {
    fn to_sql(&self, _: &Type, w: &mut Vec<u8>) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let time = match self.signed_duration_since(base()).num_microseconds() {
            Some(time) => time,
            None => return Err("value too large to transmit".into()),
        };
        types::timestamp_to_sql(time, w);
        Ok(IsNull::No)
    }

    accepts!(TIMESTAMP);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for DateTime<Utc> {
    fn from_sql(type_: &Type, raw: &[u8]) -> Result<DateTime<Utc>, Box<dyn Error + Sync + Send>> {
        let naive = NaiveDateTime::from_sql(type_, raw)?;
        Ok(DateTime::from_utc(naive, Utc))
    }

    accepts!(TIMESTAMPTZ);
}

impl ToSql for DateTime<Utc> {
    fn to_sql(
        &self,
        type_: &Type,
        w: &mut Vec<u8>,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.naive_utc().to_sql(type_, w)
    }

    accepts!(TIMESTAMPTZ);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for DateTime<Local> {
    fn from_sql(type_: &Type, raw: &[u8]) -> Result<DateTime<Local>, Box<dyn Error + Sync + Send>> {
        let utc = DateTime::<Utc>::from_sql(type_, raw)?;
        Ok(utc.with_timezone(&Local))
    }

    accepts!(TIMESTAMPTZ);
}

impl ToSql for DateTime<Local> {
    fn to_sql(
        &self,
        type_: &Type,
        w: &mut Vec<u8>,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.with_timezone(&Utc).to_sql(type_, w)
    }

    accepts!(TIMESTAMPTZ);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for DateTime<FixedOffset> {
    fn from_sql(
        type_: &Type,
        raw: &[u8],
    ) -> Result<DateTime<FixedOffset>, Box<dyn Error + Sync + Send>> {
        let utc = DateTime::<Utc>::from_sql(type_, raw)?;
        Ok(utc.with_timezone(&FixedOffset::east(0)))
    }

    accepts!(TIMESTAMPTZ);
}

impl ToSql for DateTime<FixedOffset> {
    fn to_sql(
        &self,
        type_: &Type,
        w: &mut Vec<u8>,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.with_timezone(&Utc).to_sql(type_, w)
    }

    accepts!(TIMESTAMPTZ);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for NaiveDate {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<NaiveDate, Box<dyn Error + Sync + Send>> {
        let jd = types::date_from_sql(raw)?;
        Ok(base().date() + Duration::days(i64::from(jd)))
    }

    accepts!(DATE);
}

impl ToSql for NaiveDate {
    fn to_sql(&self, _: &Type, w: &mut Vec<u8>) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let jd = self.signed_duration_since(base().date()).num_days();
        if jd > i64::from(i32::max_value()) || jd < i64::from(i32::min_value()) {
            return Err("value too large to transmit".into());
        }

        types::date_to_sql(jd as i32, w);
        Ok(IsNull::No)
    }

    accepts!(DATE);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for NaiveTime {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<NaiveTime, Box<dyn Error + Sync + Send>> {
        let usec = types::time_from_sql(raw)?;
        Ok(NaiveTime::from_hms(0, 0, 0) + Duration::microseconds(usec))
    }

    accepts!(TIME);
}

impl ToSql for NaiveTime {
    fn to_sql(&self, _: &Type, w: &mut Vec<u8>) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let delta = self.signed_duration_since(NaiveTime::from_hms(0, 0, 0));
        let time = match delta.num_microseconds() {
            Some(time) => time,
            None => return Err("value too large to transmit".into()),
        };
        types::time_to_sql(time, w);
        Ok(IsNull::No)
    }

    accepts!(TIME);
    to_sql_checked!();
}
