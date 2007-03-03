#!/usr/local/bin/ruby

require 'case_storage'
require 'higgs/index/qdbm'
require 'rubyunit'

module Higgs::StorageTest
  class StorageTransactionContextTest_QDBM < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include StorageTransactionContextTestCase

    def dbm_open
      Higgs::Index::QDBM_OPEN
    end
  end
end
