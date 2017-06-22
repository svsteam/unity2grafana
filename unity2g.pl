#!/usr/bin/perl -w
use strict;
use LWP::UserAgent;
use HTTP::Cookies;
use JSON;
use Data::Dumper;
use Time::Local ;
use IO::Socket::INET;
# copyleft under GNU GPL 3.0 or later
# fs@mond.at
#

my $datadir="/var/spool/unity2g";
my $interval=60; # check every 60 seconds. since unity api is fast, this is reasonable
                 # use a crnotjob like */1 * * * * sleep 15 ; /path/to/unity2g.pl 10.1.2.3 >> /var/log/u2g.log 2>&1
                 # 
my $IP_ADDR =  $ARGV[0];

die "unity2g.pl IP_ADDR " unless defined $IP_ADDR  ;

my $USER = 'username-of-readonly-user';
my $PASS = 'password-of-readonly-user';
my $EMC_CSRF_TOKEN;
use constant {
    GET   => 'GET',
    POST   => 'POST',
    DELETE => 'DELETE',
};

my $carbon_server = '127.11.12.13';
my $carbon_port = 8086;
my $prefix='storage.unity';

my $sock = IO::Socket::INET->new(
        PeerAddr => $carbon_server,
        PeerPort => $carbon_port,
        Proto    => 'tcp'
);
                        
die "Unable to connect: $!\n" unless ($sock->connected);
print "connected to carbon at " . localtime() . "\n";                        

my $fnlck="$datadir/$IP_ADDR" . ".lck";

if (-e $fnlck) {
  print "lock $fnlck exists\n";
  my @sta=stat $fnlck;
  my $age=time()- $sta[9];
  if ($age > 7200 ) {
    unlink($fnlck);
    print "lock removed $fnlck\n";
  }
  exit(1);
} else {
  open(L,">$fnlck") or die "can not write lock $fnlck";
  print L $$,"\n";
  close(L);
}

sub fillpath {
  my $fill=shift;
  my $tfill=shift;
  my $path=shift;
  my $hash=shift;
  my $dt=shift;
  my $ts=shift;
  
  # fill in the stars from hash, normalize for grafana  sp.*.fibreChannel.target.*.readBytes
  foreach my $k (keys %$hash) {
    my $outpath=$path;
    my $norm=$k;
    $norm =~ tr /0-9a-zA-Z\_\-//cd ;
    $outpath =~ s/\*/$norm/e;
    # print "got outpath $outpath\n";
    if (ref $hash->{$k} eq 'HASH') {
       if ( $outpath =~ m/\*/ ) {
         fillpath($fill,$tfill,$outpath,$hash->{$k},$dt,$ts); 
       } else {
         print STDERR "no * in $outpath\n";
       }  
    } else {
      my $val=$hash->{$k};
      if ($dt != 0 ) {
        $val=1.0 * $val / $dt ;
      } else {
        $val=0.0;
      }
      $fill->{$outpath}=$val;
      $tfill->{$outpath}=$ts;
      # print "$outpath => ",$hash->{$k},"\n";
    }
  }
}


sub mkdiff {
  my $now=shift;
  my $old=shift;
  foreach my $k (keys %$now) {
    #print "$k -> ",$now->{$k},"\n";
    if (defined $old->{$k}) {
      if (ref $now->{$k} eq 'HASH') {
        # print "$k points to hash\n";
        mkdiff($now->{$k},$old->{$k});
      } else {
        # print $k," -> ",$now->{$k}, " MINUS ",  $old->{$k},"\n";
        $now->{$k} -= $old->{$k} ;
      }  
    } else {
      delete $now->{$k};
      # print "deleting $k\n";
    } 
  }
}


sub ts2sec {
  my $time=shift;
  my $res;
  # 2017-06-08T11:55:00.000Z
  if ($time =~  m/^(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)\.(\d+)Z$/ ) {
    $res= timegm( $6, $5, $4, $3, ($2)-1, $1 ) ;
  }
  return $res;
}


my $ua = LWP::UserAgent->new(
             ssl_opts => { SSL_verify_mode => 'SSL_VERIFY_NONE'},
             cookie_jar => {}
);




my $json = JSON->new->allow_nonref();
sub request{
  my $type = $_[0];
  my $url = $_[1];
  my $post_data = $_[2];

  if( !defined($EMC_CSRF_TOKEN) && $type ne GET){
                     #First connection
    request(GET, 'types/loginSessionInfo');
  }
  my $req = HTTP::Request->new;
  $req->uri('https://'.$IP_ADDR.'/api/'.$url);
  $req->method($type);
  $req->header('content-type' => 'application/json');
  $req->header('accept' => 'application/json');
  $req->header('X-EMC-REST-CLIENT' => 'true');
  if(defined($EMC_CSRF_TOKEN)){
                              $req->header('EMC-CSRF-TOKEN' => $EMC_CSRF_TOKEN);
  }

  if($ua->cookie_jar->as_string eq ""){
     $req->authorization_basic($USER, $PASS);
  }
  if (defined $post_data){
     $req->content( $json->encode($post_data));
  }
  my $resp = $ua->request($req);
  #FOR DEBUG - PRINTS HEADERS
  #print $resp->headers_as_string;
  #PRINT COOKIES
  #print $ua->cookie_jar->as_string;
  if ($resp->is_success) {
    if(!defined($EMC_CSRF_TOKEN)){
      $EMC_CSRF_TOKEN = $resp->header('EMC-CSRF-TOKEN');
    }
    my $message = $resp->decoded_content;
    #print "Received reply: $message\n";
    return $json->decode($message);
  } else {
    my $res;
    $res->{'code'}=$resp->code;
    $res->{'message'}=$resp->message;
    print "HTTP error code: ", $resp->code, "\n";
    print "HTTP error message: ", $resp->message, "\n";
    return $res;
  }
}


my $sysinfo=request('GET','types/basicSystemInfo/instances');
my $name=$sysinfo->{'entries'}[0]->{'content'}->{'name'};
unlink($fnlck) unless defined $name;
die unless defined $name;
$name =~ tr /0-9a-zA-Z\_\-//cd ;
         
#my $rtq=request('POST','types/metricRealTimeQuery/instances',
#     {              
#                             "interval" => 300,
#     }                        "paths" => ['sp.*.bla','sp.*.bli']
#);


my $met=request('GET','types/metric/instances?compact=true&filter=isRealtimeAvailable eq true');
my $fn="$datadir/$name.path" ;
my $fnold="$datadir/$name.data";
my $age=0;
if (-e $fn ) {
  my @stats=stat $fn;
  $age=time()- $stats[9];
  # print "age is $age seconds\n";
}  
if (! -e  $fn or $age > 86400) {
  open(O,">$fn.new");
  foreach my $ent (@{$met->{'entries'}}) {
    my $cid=$ent->{'content'}->{'id'};
    if (defined $cid) {
      my $metr=request('GET','instances/metric/' . $cid );
      my $path=$metr->{'content'}->{'path'};
    #print Dumper($path),"\n";
      if (defined $path) {
        print O "$path\n";
      }  
    }
  }
  close(O);
  unlink($fn);
  link($fnold,$fn);
  unlink($fnold);
}  


my $idfile="$datadir/$name.qid";
my $qid=0;
if (-e $idfile) {
  open(Q,"<$idfile");
  while(<Q>) {
    chomp;
    if (/(\d+)/) {
      $qid=$1;
    } 
  }
  close(Q);
  print "got query id=$qid\n";
} 

#
#request('DELETE','instances/metricRealTimeQuery/10');

#my $rtqi=request('GET','types/metricRealTimeQuery/instances');
#print Dumper($rtqi),"\n";

#my $hini=request('GET','types/hostInitiatorPath/instances');
#my $hini=request('GET','types/hostInitiator/instances');
#my $hini=request('GET','instances/hostInitiator/15');
#print Dumper($hini),"\n";




my $rtqs=request('GET','instances/metricRealTimeQuery/' . $qid);
if (defined $rtqs->{'content'}->{'id'} and $rtqs->{'content'}->{'id'} eq $qid) {
  print "valid query id\n",
} else {
  print "invalid query id $qid\n";
  $qid=0;
}

if ($qid eq 0 ) {
  my $rtq=request('POST','types/metricRealTimeQuery/instances',
     {              
                             "interval" => $intervall,
                             "paths" => [
                                         'sp.*.cpu.summary.busyTicks',
                                         'sp.*.cpu.summary.idleTicks',
                                         'sp.*.cpu.summary.waitTicks',
                                         'sp.*.platform.storageProcessorTemperature',
                                         'sp.*.fibreChannel.fePort.*.readBlocks',
                                         'sp.*.fibreChannel.fePort.*.reads',
                                         'sp.*.fibreChannel.fePort.*.writeBlocks',
                                         'sp.*.fibreChannel.fePort.*.writes',
                                         'sp.*.blockCache.global.summary.readHits',
                                         'sp.*.blockCache.global.summary.readMisses',
                                         'sp.*.blockCache.global.summary.writeHits',
                                         'sp.*.blockCache.global.summary.writeMisses',
                                         'sp.*.cpu.uptime',
#                                         'sp.*.fibreChannel.initiator.*.readBytes',
#                                         'sp.*.fibreChannel.initiator.*.reads',
#                                         'sp.*.fibreChannel.initiator.*.totalBytes',
#                                         'sp.*.fibreChannel.initiator.*.totalCalls',
#                                         'sp.*.fibreChannel.initiator.*.totalLogins',
#                                         'sp.*.fibreChannel.initiator.*.writeBytes',
#                                         'sp.*.fibreChannel.initiator.*.writes',
                                         'sp.*.storage.summary.readBlocks',
                                         'sp.*.storage.summary.reads',
                                         'sp.*.storage.summary.writeBlocks',
                                         'sp.*.storage.summary.writes',
                                         'sp.*.storage.pool.*.sizeFree',
                                         'sp.*.storage.pool.*.sizeSubscribed',
                                         'sp.*.storage.pool.*.sizeTotal',
                                         'sp.*.storage.pool.*.sizeUsed'
#                                         'sp.*.fibreChannel.target.*.readBytes',
#                                         'sp.*.fibreChannel.target.*.reads',
#                                         'sp.*.fibreChannel.target.*.totalBytes',
#                                         'sp.*.fibreChannel.target.*.totalCalls',
#                                         'sp.*.fibreChannel.target.*.totalLogins',
#                                         'sp.*.fibreChannel.target.*.writeBytes',
#                                         'sp.*.fibreChannel.target.*.writes'
                                        ]
     }                                   
  );

  print Dumper($rtq),"\n";
  $qid=$rtq->{'content'}->{'id'};
  print "New Query installed id=$qid, need to wait for data collection now...\n";
  open(Q,">$idfile") or die "can not write $idfile $!\n";
  print Q $qid,"\n";
  close(Q);
  unlink($fnlck);
  exit(0);
}  

my $dataold; 
if ( -e $fnold) {
  my $str="";
  open(I,"<$fnold");
  while(<I>) {
    $str .= $_ ;
  }
  close(I);
  $dataold=$json->decode($str);
  # print "data old " , "-" x 40 , "\n",Dumper($dataold),"\n";
}


my $data;

#my $met=request('GET','instances/metric/14825');
my $rtquery=request('GET','types/metricQueryResult/instances?filter=queryId EQ ' . $qid);
foreach my $entr (@{$rtquery->{'entries'}}) {                           
  my $econt=$entr->{'content'};
  # print "-" x 40, "\n";
  # print Dumper($econt),"\n";
  my $path=$econt->{'path'};
  my $timestamp=$econt->{'timestamp'};
  my $ts=ts2sec($timestamp);
  $data->{$path}->{'ts'}=$ts;
  $data->{$path}->{'time'}=$timestamp;
  $data->{$path}->{'values'}=$econt->{'values'};
}

open(O,">$fnold") or die "can not write $fnold $!";
print O $json->encode($data);
close(O);


if (defined $dataold) {
  foreach my $path (keys %$data) {
    if (defined $data->{$path} ) {
      # print "path=$path\n";
      if ($path =~ m /size/ or $path =~ m/Temperature/ or $path =~ m/uptime/ ) {
        # print "no diff necessary\n";
      } else {
        # print "doing diff for $path\n";
        mkdiff($data->{$path}->{'values'},$dataold->{$path}->{'values'});
        $data->{$path}->{'dt'} = $data->{$path}->{'ts'} - $dataold->{$path}->{'ts'}
        #print Dumper($data->{$path}->{'values'}),"\n";
        #print Dumper($dataold->{$path}->{'values'}),"\n";
      }
    }
  }
  my %fillr ;
  my %tfillr;
  my $fill=\%fillr;
  my $tfill=\%tfillr;
  foreach my $path (keys %$data) {
    if (defined $data->{$path} ) {
      my $dt=$data->{$path}->{'dt'};
      my $ts=$data->{$path}->{'ts'};
      $dt=1 unless defined $dt;
      # print "fillpath $path $dt $ts\n";
      if (defined $dt and $dt > 0 ) {
        if ( $path =~ m /size/ or $path =~ m/Temperature/ or $path =~ m/uptime/ or $path =~ m/Ticks/ ) {
          $dt=1;
        }
        fillpath($fill,$tfill,$path,$data->{$path}->{'values'},$dt,$ts);
      }  
    }
  }
  foreach my $p (keys %$fill) {
    if ( $p =~ m/^(.*)\.cpu\.summary.busyTicks/ ) {
      my $start=$1;
      # print "tick hack $start for $p\n";
      my $bt=$fill->{$start . '.cpu.summary.busyTicks'};
      my $it=$fill->{$start . '.cpu.summary.idleTicks'};
      my $wt=$fill->{$start . '.cpu.summary.waitTicks'};
      
      delete $fill->{$start . '.cpu.summary.waitTicks'};
      delete $fill->{$start . '.cpu.summary.idleTicks'};
      delete $fill->{$start . '.cpu.summary.busyTicks'};
      
      # print "tick hack values bt=$bt it=$it wt=$wt\n";
      if (defined $bt and defined $it and defined $wt ) {
        my $ticks=$bt+$it+$wt;
        if ($ticks > 0 ) {
          my $ts=$tfill->{$start . '.cpu.summary.busyTicks'};
          
          $fill->{$start . '.cpu.busy'} = $bt / $ticks * 1.0; 
          $fill->{$start . '.cpu.wait'} = $wt / $ticks * 1.0; 
          $fill->{$start . '.cpu.idle'} = $it / $ticks * 1.0; 
          $tfill->{$start . '.cpu.busy'} = $ts;
          $tfill->{$start . '.cpu.wait'} = $ts;
          $tfill->{$start . '.cpu.idle'} = $ts;
        }
      }      
    }
    # print "path $p -> ",$fill->{$p},"\n";
  }
  my $wcnt=0;
  foreach my $p (keys %$fill) {
    # print "path $p -> ",$fill->{$p},"\n";
    my $val=$fill->{$p};
    my $ts=$tfill->{$p};
    if (defined $ts) {
      my $path=$prefix . "." . $name . ".$p";
#      print "$path $val $ts\n";
      print $sock "$path $val $ts\n";
      $wcnt++;
    } else {
      print "timestamp not defined for $p and value $val\n";
    }  
  }
  print "written $wcnt metrics for $name " . localtime() . "\n";
}  

unlink($fnlck);

# print Dumper($data),"\n";



