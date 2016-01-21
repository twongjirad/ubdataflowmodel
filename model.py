import numpy as np

class datapacket:
    def __init__(self,events, mb_per_events=40):
        self.events = events
        self.mb_per_events = mb_per_events
        self.size_mb = self.mb_per_events*events

class node_daq:
    def __init__(self, event_per_sec, mb_per_event=40 ):
        self.event_per_sec = event_per_sec
        self.mb_per_event = mb_per_event
    def gendata( self, tlen_sec ):
        l = self.event_per_sec*tlen_sec
        nevents = np.random.poisson(l)
        return datapacket(nevents)

class node_enstore:
    def __init__(self,maxpoolsize_mb,recosize_mb,rawsize_mb):
        self.raw_nevents = 0
        self.raw_size_mb = 0
        self.raw_eventsize_mb = rawsize_mb
        self.reco_nevents = 0
        self.reco_size_mb = 0
        self.reco_eventsize_mb = recosize_mb
        self.maxpoolsize_mb = maxpoolsize_mb
    def receive_daq( self, packet ):
        self.raw_nevents += packet.events
        self.raw_size_mb += packet.size_mb
    def receive_grid( self, packet ):
        self.reco_nevents += packet.events
        self.reco_size_mb += packet.size_mb
    def getreco_for_tape(self,maxevents):
        totstore = self.reco_size_mb+self.raw_size_mb
        if totstore<self.maxpoolsize_mb:
            return datapacket(0) # wait, so tape can spend as much as possible to tranfer raw
        else:
            nreco_out = np.minimum( maxevents, self.reco_nevents )
            
            self.reco_nevents -= nreco_out
            self.reco_size_mb -= nreco_out*self.reco_eventsize_mb
            return datapacket( nreco_out, self.reco_eventsize_mb )

    def getraw_for_grid(self,nevents):
        send_events = np.minimum( nevents, self.raw_nevents )
        self.raw_nevents -= send_events
        self.raw_size_mb -= send_events*self.raw_eventsize_mb
        return datapacket( send_events, self.raw_eventsize_mb )
    def isfull(self):
        if self.raw_size_mb+self.reco_size_mb>self.maxpoolsize_mb:
            return True
        else:
            return False
        

class node_grid:
    def __init__(self, nmaxworkers, rate_per_event, recosize_mb):
        self.neventsqueued = 0
        self.nmaxworkers = nmaxworkers
        self.rate_per_event = rate_per_event
        self.recosize_mb = recosize_mb
    def processdata( self, tstep ):
        if self.neventsqueued==0:
            return datapacket(0,0)
        expectedfinished = int(tstep*self.rate_per_event*self.nmaxworkers)
        # use steady rate, not poisson fluctuation for now
        if self.neventsqueued>expectedfinished:
            self.neventsqueued -= expectedfinished
            return datapacket( expectedfinished, self.recosize_mb )
        else:
            # clear out!
            send = self.neventsqueued
            self.neventsqueued = 0
            return datapacket( send, self.recosize_mb )
    def queueevents( self, packet ):
        self.neventsqueued += packet.events
    def workers_available( self ):
        # this isn't right. i would technically have to sim launching of jobs.
        # but for an average throughput, maybe this is ok.
        return np.maximum( 0, self.nmaxworkers-self.neventsqueued )

class edge_totape:
    def __init__(self,maxthrougput):
        self.maxthroughput = maxthroughput

class node_tape:
    def __init__(self,event_backlog, rawsize_mb):
        self.event_backlog = event_backlog
        self.event_processed = 0
        self.rawsize_mb = rawsize_mb
    def getbacklog(self,nevents):
        send = np.minimum( self.event_backlog, nevents )
        self.event_backlog -= send
        return datapacket( send, self.rawsize_mb )
    def receive_reco(self,packet):
        self.event_processed += packet.events

if __name__ == "__main__":

    DAYS_OF_BACKLOG = 120
    daqrate_hz = 0.5 # hz
    raw_mb_per_event = 40
    recosize_mb = 120
    time_step_sec = 10.0 # secs
    NWORKERS = 1000
    TAPE_BANDWIDTH = 450 # mb/s
    reco_rate_hz_perevent = 1.0/800.0 # sec
    event_backlog = 5.0*(3600)*24*(DAYS_OF_BACKLOG)
    maxpool_mb = 50*1e6 # TB to mb

    # setup components
    daq = node_daq( daqrate_hz, raw_mb_per_event )
    grid = node_grid( NWORKERS, reco_rate_hz_perevent, recosize_mb )
    tape = node_tape( event_backlog, raw_mb_per_event )
    enstore = node_enstore( maxpool_mb, recosize_mb, raw_mb_per_event )


    tottime = 0.0
    last_report = 0.0
    nsteps = 0
    while True:
        tottime += time_step_sec
        nsteps += 1
        # step 1, gen packets from daq and grid

        # DAQ outputs raw binary events
        packet_raw_from_daq = daq.gendata(time_step_sec)
        # grid processes from files
        packet_reco_from_grid = grid.processdata( time_step_sec )

        # step 2, enstore transactions from daq and grid
        # receive daq packets (raw)
        enstore.receive_daq( packet_raw_from_daq )
        # receive grid packets (reco)
        enstore.receive_grid( packet_reco_from_grid )

        # step 3 optional enstore transactions
        # enstore pushes reco to tape if needed
        max_reco_events = int( (TAPE_BANDWIDTH*time_step_sec)/recosize_mb )
        packet_reco_fromenstore_totape = enstore.getreco_for_tape(max_reco_events)

        # d) use remaining bandwidth to pull (raw) events from tape
        reco_bandwidth_used = packet_reco_fromenstore_totape.size_mb
        nevents_raw_leftover = int( (TAPE_BANDWIDTH*time_step_sec-reco_bandwidth_used)/recosize_mb )
        if nevents_raw_leftover<0 or enstore.isfull():
            nevents_raw_leftover = 0
        packet_raw_fromtape = tape.getbacklog( nevents_raw_leftover )
        enstore.receive_daq( packet_raw_fromtape ) # same as daq it's raw
        # e) enstore sends reco events to tape
        if packet_reco_fromenstore_totape.events>0:
            tape.receive_reco( packet_reco_fromenstore_totape )
        # f) enstore sends raw events to grid
        navailable = grid.workers_available()
        if navailable>0:
            packet_raw_fromenstore_togrid = enstore.getraw_for_grid(navailable)
            grid.queueevents( packet_raw_fromenstore_togrid )

        # REPORT
        if (tottime - last_report)/(3600.0*24.0) > (5.0):
            print "time=",tottime/(3600*24)," days"
            print " [DAQ] sent=",packet_raw_from_daq.events
            print " [GRID] finished=",packet_reco_from_grid.events," processed from grid and sent to enstore",
            print " ",time_step_sec*reco_rate_hz_perevent*NWORKERS
            print " [ENSTORE] raw=",enstore.raw_size_mb," (",enstore.raw_nevents,") reco=",enstore.reco_size_mb," (",enstore.reco_nevents,")",
            print "  %.2f"%( 100.0*(enstore.raw_size_mb+enstore.reco_size_mb)/enstore.maxpoolsize_mb )
            print " [TAPE] event backlog=",tape.event_backlog," processed=",tape.event_processed
            print " [SEND ]",packet_reco_fromenstore_totape.events," overflow reco from enstore to tape (",
            print " (%.2f%%)"%(100.0*reco_bandwidth_used/(TAPE_BANDWIDTH*time_step_sec))
            print " [SEND] ",packet_raw_fromtape.events," raw events from tape to enstore"
            print " [SEND] ",navailable," raw events from enstore to grid"
            print " [GRID] workers available=",grid.workers_available()," events queued=",grid.neventsqueued
            if enstore.isfull():
                print "!! [ENSTORE FULL] !!"
            last_report = tottime

        if tottime/(3600.0*24.0)>120:
            break
        #if nsteps>=100:
        #    break
