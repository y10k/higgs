# storage interface like dbm
#
# Author:: $Author$
# Date:: $Date$
# Revision:: $Revision$
#

require 'higgs/storage'
require 'higgs/tman'

module Higgs
  # storage interface like dbm
  #
  # ex: sample/dbmtest.rb
  #   :include: sample/dbmtest.rb
  #
  class DBM
    # for ident(1)
    CVS_ID = '$Id$'

    include Storage::Export
    include TransactionManager::Export

    DECODE = proc{|r| r }
    ENCODE = proc{|w| w }

    # <tt>name</tt> is a storage name and see Higgs::Storage for detail.
    # see Higgs::Storage and Higgs::TransactionManager for <tt>options</tt>.
    def initialize(name, options={})
      @storage = Storage.new(name, options)
      options[:decode] = DECODE
      options[:encode] = ENCODE
      @tman = TransactionManager.new(@storage, options)
    end

    def self.open(*args)
      dbm = new(*args)
      begin
        r = yield(dbm)
      ensure
        dbm.shutdown
      end
      r
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
