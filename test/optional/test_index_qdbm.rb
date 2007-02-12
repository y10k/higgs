#!/usr/local/bin/ruby

require 'higgs/index/qdbm'
require 'rubyunit'
require 'utils_index'

module Higgs::IndexTest
  # for ident(1)
  QDBM_CVS_ID = '$Id$'

  class QDBMTest < RUNIT::TestCase
    include Higgs::Index
    include IndexTest

    def db_read_open
      db = QDBM_READ_OPEN.call(@name)
      begin
        r = yield(db)
      ensure
        db.close
      end
      r
    end

    def db_write_open
      db = QDBM_WRITE_OPEN.call(@name)
      begin
        r = yield(db)
      ensure
        db.close
      end
      r
    end
  end
end
