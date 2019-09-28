pub enum TryIterator<T, E, I>
where
    I: Iterator<Item = Result<T, E>>,
{
    Err(Option<E>),
    Iter(I),
}

impl<T, E, I> Iterator for TryIterator<T, E, I>
where
    I: Iterator<Item = Result<T, E>>,
{
    type Item = Result<T, E>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Err(v @ Some(_)) => Some(Err(v.take().unwrap())),
            Self::Err(None) => None,
            Self::Iter(i) => i.next(),
        }
    }
}

// #![feature(try_trait)]
// impl<T, E, I> std::ops::Try for TryIterator<T, E, I>
// where
//     I: Iterator<Item = Result<T, E>>,
// {
//     type Ok = I;
//     type Error = E;

//     #[inline]
//     fn into_result(self) -> Result<Self::Ok, Self::Error> {
//         match self {
//             Self::Err(e) => Err(e.unwrap()),
//             Self::Iter(i) => Ok(i),
//         }
//     }

//     #[inline]
//     fn from_ok(v: Self::Ok) -> Self {
//         Self::Iter(v)
//     }

//     #[inline]
//     fn from_error(e: Self::Error) -> Self {
//         Self::Err(Some(e))
//     }
// }
