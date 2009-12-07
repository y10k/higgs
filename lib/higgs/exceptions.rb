# -*- coding: utf-8 -*-
# = exceptions
# == license
#   :include:../LICENSE
#

module Higgs
  module Exceptions
    class HiggsError < StandardError
    end

    class HiggsException < Exception
    end

    class ShutdownException < HiggsException
    end
  end
  include Exceptions
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
