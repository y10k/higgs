#!/usr/local/bin/ruby

require 'higgs/index/qdbm'
require 'rubyunit'
require 'utils_storage'

module Higgs::StorageTest
  class StorageQDBMTest < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include StorageTest

    def dbm_open
      Higgs::Index::QDBM_OPEN
    end
  end
end
