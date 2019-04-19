load ../../../common

@test "kinesis/test_strict_order_shared" {
  SORT=false do_test
}