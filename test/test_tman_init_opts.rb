#!/usr/local/bin/ruby

require 'higgs/tman'
require 'test/unit'

module Higgs::Test
  class TransactionManagerInitOptionsTest < Test::Unit::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include Higgs::TransactionManager::InitOptions

    def test_init_options_default
      init_options({})
      assert_equal(false, @read_only)
      assert_equal(false, self.read_only)
      assert_equal(:foo, @decode.call(:foo))
      assert_equal(:bar, @encode.call(:bar))
      assert_instance_of(Higgs::GiantLockManager, @lock_manager) # auto: require 'higgs/lock'
      assert_instance_of(Higgs::LRUCache, @master_cache) # auto: require 'higgs/cache'
    end
  end
end
