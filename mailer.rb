require 'redis'
require 'json'
require 'pony'
require 'mysql2'

trap("INT") {
  puts "=== wait for child process to exit ..."
  wait
  puts "=== XXX Mailer gracefully exit"
  exit
}

module RedisConf
  Host = '127.0.0.1'
  Port = 6379
  DB   = 8
  List = "validation"
end

module PonyConf
  Charset = 'utf-8'
  SMTP_Options = {
    :address        => 'smtp.mxhichina.com',
    :port           => '25',
    :user_name      => 'noreply@hanfeizishuo.com',
    :password       => '#########',
    :authentication => :plain,
    :domain         => "mail.hanfeizishuo.com"
  }
end

begin
  puts "[mailer] #{Time.now} : init Redis client ..."
  rc = Redis.new(
    :host => RedisConf::Host,
    :port => RedisConf::Port,
    :db   => RedisConf::DB
  )
  raise "[mailer] #{Time.now} : cannot create Redis client" unless rc

  puts "[mailer] #{Time.now} : init Mysql2 client ... "
  mc = Mysql2::Client.new(
    :host     => 'localhost',
    :username => 'dave',
    :password => '########',
    :database => 'xxx'
  )
  raise "[mailer] #{Time.now} : cannot create Mysql2 client" unless mc
rescue
  puts $!.message
  puts "[mailer] #{Time.now} : exit ..."
  exit
end

include Process

pid = fork do
  trap("INT") {
    puts "get ctrl-c, exit ..."
    exit
  }

  trap("TERM") {
    puts "get TERM signal, exit ..."
    exit
  }

  while true
    begin
      puts "[mailer] #{Time.now} : waiting for next job ..."

      job = rc.brpop("validation")
      puts "[mailer] #{Time.now} : job get =>"

      mail = JSON.parse(job[1])
      puts "  list name: #{job[0]}, mail: #{mail}"
      puts "  gonna send mail to #{mail["to"]} ..."

      Pony.mail(
        :to          => mail["to"],
        :from        => mail["from"],
        :subject     => mail["subject"],
        :html_body   => mail["body"],
        :charset     => PonyConf::Charset,
        :via         => :smtp,
        :via_options => PonyConf::SMTP_Options
      )

      puts "  gonna update status in mail_logs for id = #{ mail["log_id"] } ..."
      mc.query("UPDATE mail_logs SET status = 2, updated_at = now() WHERE id = #{ mail["log_id"] }")
      puts "  mail sent successfully!"
    rescue
      puts "[mailer] Exception in waiting loop:"
      puts $!.message
    end
  end
end

wait
