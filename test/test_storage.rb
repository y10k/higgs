#!/usr/local/bin/ruby

require 'higgs/storage'
require 'rubyunit'

module Higgs::StorageTest
  # for ident(1)
  CVS_ID = '$Id$'

  class InitOptionsTest < RUNIT::TestCase
    include Higgs::Storage::InitOptions

    def test_init_options_default
      init_options({})
      assert_equal(false, @read_only)
      assert_equal(2, @number_of_read_io)
      assert_equal(Higgs::Index::GDBM_OPEN[:read], @dbm_read_open)
      assert_equal(Higgs::Index::GDBM_OPEN[:write], @dbm_write_open)
      assert_instance_of(Higgs::Lock::FineGrainLockManager, @lock_manager)
    end
  end
end
