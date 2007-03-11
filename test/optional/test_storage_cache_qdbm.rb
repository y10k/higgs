#!/usr/local/bin/ruby

require 'case_storage'
require 'higgs/index/qdbm'
require 'rubyunit'

module Higgs::StorageTest
  class StorageCacheManagerTest_QDBM < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include StorageTestCase

    def dbm_open
      Higgs::Index::QDBM_OPEN
    end

    def storage_type
      Storage::CacheManager
    end
  end
end
