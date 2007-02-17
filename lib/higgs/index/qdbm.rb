# $Id$

require 'depot'
require 'higgs/index'

module Higgs
  module Index
    # for ident(1)
    QDBM_CVS_ID = '$Id$'

    QDBM_R_FLAGS = Depot::OREADER
    QDBM_W_FLAGS = Depot::OWRITER | Depot::OCREAT

    module DepotHasKey
      def key?(key)
        fetch(key, :none) != :none
      end

      alias has_key? key?
    end

    QDBM_OPEN = {
      :read => proc{|name|
        db = Depot.open(name, QDBM_R_FLAGS)
        db.silent = true
        db.extend(DepotHasKey)
      },
      :write => proc{|name|
        db = Depot.open(name, QDBM_W_FLAGS)
        db.silent = true
        db.extend(DepotHasKey)
      }
    }
  end
end
