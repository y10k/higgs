#!/usr/local/bin/ruby

require 'higgs/index/qdbm'
require 'rubyunit'
require 'utils_index'

module Higgs::IndexTest
  class QDBMTest < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include Higgs::Index
    include IndexTest

    def db_read_open
      db = QDBM_OPEN[:read].call(@name)
      begin
        r = yield(db)
      ensure
        db.close
      end
      r
    end

    def db_write_open
      db = QDBM_OPEN[:write].call(@name)
      begin
        r = yield(db)
      ensure
        db.close
      end
      r
    end
  end
end
