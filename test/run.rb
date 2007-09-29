#!/usr/local/bin/ruby

$: << File.dirname(__FILE__)
$: << File.join(File.dirname(__FILE__), '..', 'lib')

# for ident(1)
CVS_ID = '$Id$'

mask = //                       # any match
if ($0 == __FILE__) then
  if (ARGV.length > 0 && ARGV[0] !~ /^-/) then
    mask = Regexp.compile(ARGV.shift)
  end
end

test_dir, this_name = File.split(__FILE__)
for test_rb in Dir.entries(test_dir).sort
  case (test_rb)
  when this_name
    # skip
  when /^test_.*\.rb$/
    if (test_rb =~ mask) then
      puts "load #{test_rb}"
      require File.join(test_dir, test_rb)
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
