# storage interface like dbm

require 'higgs/storage'
require 'higgs/tman'

module Higgs
  # storage interface like dbm
  class DBM
    # for ident(1)
    CVS_ID = '$Id$'

    extend Forwardable
    include Storage::Export
    include TransactionManager::Export

    DECODE = proc{|r| r }
    ENCODE = proc{|w| w }

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
