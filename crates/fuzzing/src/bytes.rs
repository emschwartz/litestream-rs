//! Reading fuzz input without indexing, and the one way a target fails.

/// A cursor over fuzz bytes. Every read is checked: short input is `None`.
pub(crate) struct Reader<'a> {
    data: &'a [u8],
}

impl<'a> Reader<'a> {
    pub(crate) const fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub(crate) fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let (head, tail) = self.data.split_at_checked(len)?;
        self.data = tail;
        Some(head)
    }

    pub(crate) fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        <[u8; N]>::try_from(self.take(N)?).ok()
    }

    pub(crate) fn u8(&mut self) -> Option<u8> {
        self.array::<1>().map(|[byte]| byte)
    }

    pub(crate) fn u16(&mut self) -> Option<u16> {
        self.array::<2>().map(u16::from_be_bytes)
    }

    pub(crate) fn u32(&mut self) -> Option<u32> {
        self.array::<4>().map(u32::from_be_bytes)
    }

    /// Everything not yet read.
    pub(crate) fn rest(&mut self) -> &'a [u8] {
        std::mem::take(&mut self.data)
    }
}

/// Reports a broken contract. A target that panics is a finding: libFuzzer
/// saves the input, and the corpus test fails.
#[expect(
    clippy::panic,
    reason = "a panic is how a fuzz target reports a violated property"
)]
pub fn violation(what: &str) -> ! {
    panic!("contract violated: {what}")
}
