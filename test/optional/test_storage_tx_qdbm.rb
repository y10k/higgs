#!/usr/local/bin/ruby

require 'higgs/index/qdbm'
require 'rubyunit'
require 'utils_storage'

module Higgs::StorageTest
  class StorageTransactionHandlerQDBMTest < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include StorageTransactionHandlerTest

    def dbm_open
      Higgs::Index::QDBM_OPEN
    end
  end
end
