//! The Stream service WIT that each SDK copies and generates its bindings from.

const WIT: &str = include_str!("../nexus/stream-service.wit");
const WRITER_WIT: &str = include_str!("../nexus/stream-writer.wit");

#[test]
fn every_wit_line_fits_100_columns() {
    for wit in [WIT, WRITER_WIT] {
        let long: Vec<_> = wit
            .lines()
            .enumerate()
            .filter(|(_, line)| line.chars().count() > 100)
            .map(|(n, _)| n + 1)
            .collect();
        assert!(long.is_empty(), "lines over 100 columns: {long:?}");
    }
}
