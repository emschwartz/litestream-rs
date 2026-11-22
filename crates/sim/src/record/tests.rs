use super::*;

#[test]
fn an_operation_is_written_as_its_constructor() {
    let fault = Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::Upload,
        ahead: 2,
        shape: FaultShape::Error(StoreError::Throttled {
            op: StoreOp::Upload,
            retry_after: Some(Duration::from_millis(1500)),
        }),
    };
    assert_eq!(
        op(&fault).unwrap(),
        "Op::StoreFault { target: Target::Remote, op: StoreOp::Upload, ahead: 2, shape: \
         FaultShape::Error(StoreError::Throttled { op: StoreOp::Upload, retry_after: \
         Some(Duration::from_millis(1500)) }) }"
    );
    assert_eq!(
        op(&Op::StagingDamage(MetaOp::Restore)).unwrap(),
        "Op::StagingDamage(MetaOp::Restore)"
    );
    assert_eq!(
        op(&Op::Vacuum {
            page_size: PageSize::new(2048)
        })
        .unwrap(),
        "Op::Vacuum { page_size: PageSize::new(2048) }"
    );
}

#[test]
fn the_error_domain_s_store_errors_are_written_as_their_constructors() {
    let denied = Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::List,
        ahead: 0,
        shape: FaultShape::Error(StoreError::Denied {
            op: StoreOp::List,
            why: lsr_core::Denied::Permission,
        }),
    };
    assert_eq!(
        op(&denied).unwrap(),
        "Op::StoreFault { target: Target::Staging, op: StoreOp::List, ahead: 0, shape: \
         FaultShape::Error(StoreError::Denied { op: StoreOp::List, why: Denied::Permission }) }"
    );
    let refused = FaultShape::Error(StoreError::Refused {
        op: StoreOp::Upload,
        status: 411,
    });
    assert_eq!(
        shape(&refused).unwrap(),
        "FaultShape::Error(StoreError::Refused { op: StoreOp::Upload, status: 411 })"
    );
    let source = module(&["doc"], &[denied]).unwrap();
    assert!(
        source.contains("use lsr_core::{Denied, StoreError, StoreOp};"),
        "{source}"
    );
}

#[test]
fn a_short_read_is_written_as_a_const_page_number() {
    let pgno = lsr_ltx::PageNo::new(4, PageSize::new(4096).unwrap()).unwrap();
    assert_eq!(
        source_error(SourceError::Short { pgno }).unwrap(),
        "SourceError::Short { pgno: PageNo::new(4, PageSize::new(4096).unwrap()).unwrap() }"
    );
}

#[test]
fn a_value_with_no_writer_is_refused() {
    let denied = Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::Read,
        ahead: 0,
        shape: FaultShape::Error(StoreError::NotFound {
            key: lsr_core::FileKey::snapshot(lsr_ltx::Txid::MIN),
        }),
    };
    op(&denied).unwrap_err();
    module(&["doc"], &[denied]).unwrap_err();
}
