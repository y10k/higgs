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

    QDBM_READ_OPEN = proc{|name| Depot.open(name, QDBM_R_FLAGS).extend(DepotHasKey) }
    QDBM_WRITE_OPEN = proc{|name| Depot.open(name, QDBM_W_FLAGS).extend(DepotHasKey) }
  end
end
