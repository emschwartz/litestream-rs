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
