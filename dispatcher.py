#   _________________________________________________________________
#   Comptuer Science: COMP6032
#   RoberUber Coursework
#   19129576
#   come219
#   __________________________________________________________________

#   _____________________________
#   Imported Packages
#   _____________________________
import math
import numpy
import heapq

# a data container for all pertinent information related to fares. (Should we
# add an underway flag and require taxis to acknowledge collection to the dispatcher?)
class FareEntry:

      def __init__(self, origin, dest, time, price=0, taxiIndex=-1):

          self.origin = origin
          self.destination = dest
          self.calltime = time
          self.price = price
          # the taxi allocated to service this fare. -1 if none has been allocated
          self.taxi = taxiIndex
          # a list of indices of taxis that have bid on the fare.
          self.bidders = []

          # self acknowledge collection to dispatcher ??

'''
A Dispatcher is a static agent whose job is to allocate fares amongst available taxis. Like the taxis, all
the relevant functionality happens in ClockTick. The Dispatcher has a list of taxis, a map of the service area,
and a dictionary of active fares (ones which have called for a ride) that it can use to manage the allocations.
Taxis bid after receiving the price, which should be decided by the Dispatcher, and once a 'satisfactory' number
of bids are in, the dispatcher should run allocateFare in its world (parent) to inform the winning bidder that they
now have the fare.
'''
class Dispatcher:

      # constructor only needs to know the world it lives in, although you can also populate its knowledge base
      # with taxi and map information.
      def __init__(self, parent, taxis=None, serviceMap=None):

          self._parent = parent
          # our incoming account
          self._revenue = 0
          # the list of taxis
          self._taxis = taxis
          if self._taxis is None:
             self._taxis = []
          # fareBoard will be a nested dictionary indexed by origin, then destination, then call time.
          # Its values are FareEntries. The nesting structure provides for reasonably fast lookup; it's
          # more or less a multi-level hash.
          self._fareBoard = {}
          # serviceMap gives the dispatcher its service area
          self._map = serviceMap

      #_________________________________________________________________________________________________________
      # methods to add objects to the Dispatcher's knowledge base
      
      # make a new taxi known.
      def addTaxi(self, taxi):
          if taxi not in self._taxis:
             self._taxis.append(taxi)

      # incrementally add to the map. This can be useful if, e.g. the world itself has a set of
      # nodes incrementally added. It can then call this function on the dispatcher to add to
      # its map
      def addMapNode(self, coords, neighbours):
          if self._parent is None:
             return AttributeError("This Dispatcher does not exist in any world")
          node = self._parent.getNode(coords[0],coords[1])
          if node is None:
             return KeyError("No such node: {0} in this Dispatcher's service area".format(coords))
          # build up the neighbour dictionary incrementally so we can check for invalid nodes.
          neighbourDict = {}
          for neighbour in neighbours:
              neighbourCoords = (neighbour[1], neighbour[2])
              neighbourNode = self._parent.getNode(neighbour[1],neighbour[2])
              if neighbourNode is None:
                 return KeyError("Node {0} expects neighbour {1} which is not in this Dispatcher's service area".format(coords, neighbour))
              neighbourDict[neighbourCoords] = (neighbour[0],self._parent.distance2Node(node, neighbourNode))
          self._map[coords] = neighbourDict

      # importMap gets the service area map, and can be brought in incrementally as well as
      # in one wodge.
      def importMap(self, newMap):
          # a fresh map can just be inserted
          if self._map is None:
             self._map = newMap
          # but importing a new map where one exists implies adding to the
          # existing one. (Check that this puts in the right values!)
          else:
             for node in newMap.items():
                 neighbours = [(neighbour[1][0],neighbour[0][0],neighbour[0][1]) for neighbour in node[1].items()]
                 self.addMapNode(node[0],neighbours)

      # any legacy fares or taxis from a previous dispatcher can be imported here - future functionality,
      # for the most part
      def handover(self, parent, origin, destination, time, taxi, price):
          if self._parent == parent:
             # handover implies taxis definitely known to a previous dispatcher. The current
             # dispatcher should thus be made aware of them
             if taxi not in self._taxis:
                self._taxis.append(taxi)
             # add any fares found along with their allocations
             self.newFare(parent, origin, destination, time)
             self._fareBoard[origin][destination][time].taxi = self._taxis.index(taxi)
             self._fareBoard[origin][destination][time].price = price

      #--------------------------------------------------------------------------------------------------------------
      # runtime methods used to inform the Dispatcher of real-time events


      # fares will call this when they appear to signal a request for service.
      def newFare(self, parent, origin, destination, time):
          # only add new fares coming from the same world
          if parent == self._parent:
             fare = FareEntry(origin,destination,time)
             if origin in self._fareBoard:               
                if destination not in self._fareBoard[origin]:
                   self._fareBoard[origin][destination] = {}
             else:
                self._fareBoard[origin] = {destination: {}}
             # overwrites any existing fare with the same (origin, destination, calltime) triplet, but
             # this would be equivalent to saying it was the same fare, at least in this world where
             # a given Node only has one fare at a time.
             self._fareBoard[origin][destination][time] = fare
             
      # abandoning fares will call this to cancel their request
      def cancelFare(self, parent, origin, destination, calltime):
          # if the fare exists in our world,
          if parent == self._parent and origin in self._fareBoard:
             if destination in self._fareBoard[origin]:
                if calltime in self._fareBoard[origin][destination]:
                   # get rid of it
                   print("Fare ({0},{1}) cancelled".format(origin[0],origin[1]))
                   # inform taxis that the fare abandoned
                   self._parent.cancelFare(origin, self._taxis[self._fareBoard[origin][destination][calltime].taxi])
                   del self._fareBoard[origin][destination][calltime]
                if len(self._fareBoard[origin][destination]) == 0:
                   del self._fareBoard[origin][destination]
                if len(self._fareBoard[origin]) == 0:
                   del self._fareBoard[origin]

      # taxis register their bids for a fare using this mechanism
      def fareBid(self, origin, taxi):
          # rogue taxis (not known to the dispatcher) can't bid on fares
          if taxi in self._taxis:
             # everyone else bids on fares available
             if origin in self._fareBoard:
                for destination in self._fareBoard[origin].keys():
                    for time in self._fareBoard[origin][destination].keys():
                        # as long as they haven't already been allocated
                        if self._fareBoard[origin][destination][time].taxi == -1:
                           self._fareBoard[origin][destination][time].bidders.append(self._taxis.index(taxi))
                           # only one fare per origin can be actively open for bid, so
                           # immediately return once we[ve found it
                           return
                     
      # fares call this (through the parent world) when they have reached their destination
      def recvPayment(self, parent, amount):
          # don't take payments from dodgy alternative universes
          if self._parent == parent:
             self._revenue += amount


      #________________________________________________________________________________________________________________

      # clockTick is called by the world and drives the simulation for the Dispatcher. It must, at minimum, handle the
      # 2 main functions the dispatcher needs to run in the world: broadcastFare(origin, destination, price) and
      # allocateFare(origin, taxi).
      def clockTick(self, parent):
          if self._parent == parent:
             for origin in self._fareBoard.keys():
                 for destination in self._fareBoard[origin].keys():

                     # TODO - if you can come up with something better. Not essential though.
                     # not super-efficient here: need times in order, dictionary view objects are not
                     # sortable because they are an iterator, so we need to turn the times into a
                     # sorted list. Hopefully fareBoard will never be too big
                     for time in sorted(list(self._fareBoard[origin][destination].keys())):
                         if self._fareBoard[origin][destination][time].price == 0:
                            self._fareBoard[origin][destination][time].price = self._costFare(self._fareBoard[origin][destination][time])
                            # broadcastFare actually returns the number of taxis that got the info, if you
                            # wish to use that information in the decision over when to allocate
                            self._parent.broadcastFare(origin,
                                                       destination,
                                                       self._fareBoard[origin][destination][time].price)
                         elif self._fareBoard[origin][destination][time].taxi < 0 and len(self._fareBoard[origin][destination][time].bidders) > 0:
                              self._allocateFare(origin, destination, time)

      #----------------------------------------------------------------------------------------------------------------

      ''' HERE IS THE PART THAT YOU NEED TO MODIFY
      '''

      '''this internal method should decide a 'reasonable' cost for the fare. Here, the computation
         is trivial: add a fixed cost (representing a presumed travel time to the fare by a given
         taxi) then multiply the expected travel time by the profit-sharing ratio. Better methods
         should improve the expected number of bids and expected profits. The function gets all the
         fare information, even though currently it's not using all of it, because you may wish to
         take into account other details.
      '''
      # TODO - improve costing
      def _costFare(self, fare):
          timeToDestination = self._parent.travelTime(self._parent.getNode(fare.origin[0],fare.origin[1]),
                                                      self._parent.getNode(fare.destination[0],fare.destination[1]))


          #define a better pricing

          # bettervalue = (expected profits + expected number of bids ) / (actual profit + bid)

          # if the world is gridlocked, a flat fare applies.
          if timeToDestination < 0:
             return 150

             #return bettervalue


          # return ((better value)+timetodestination)/0.9)
          return (25+timeToDestination)/0.9

      # TODO
      # this method decides which taxi to allocate to a given fare. The algorithm here is not a fair allocation
      # scheme: taxis can (and do!) get starved for fares, simply because they happen to be far away from the
      # action. You should be able to do better than that using some form of CSP solver (this is just a suggestion,
      # other methods are also acceptable and welcome).
      def _allocateFare(self, origin, destination, time):
           # a very simple approach here gives taxis at most 5 ticks to respond, which can
           # surely be improved upon.
          if self._parent.simTime-time > 5:
             allocatedTaxi = -1
             winnerNode = None
             fareNode = self._parent.getNode(origin[0],origin[1])
             # this does the allocation. There are a LOT of conditions to check, namely:
             # 1) that the fare is asking for transport from a valid location;
             # 2) that the bidding taxi is in the dispatcher's list of taxis
             # 3) that the taxi's location is 'on-grid': somewhere in the dispatcher's map
             # 4) that at least one valid taxi has actually bid on the fare


            #contraint satisfaction problem solver


             #constraint 1
             if fareNode is not None:
                for taxiIdx in self._fareBoard[origin][destination][time].bidders:
                    if len(self._taxis) > taxiIdx:
                       bidderLoc = self._taxis[taxiIdx].currentLocation
                       bidderNode = self._parent.getNode(bidderLoc[0],bidderLoc[1])

             #constraint 4
                       if bidderNode is not None:
                          # ultimately the naive algorithm chosen is which taxi is the closest. This is patently unfair for several
                          # reasons, but does produce *a* winner.


             #constraint 2
                          if winnerNode is None or self._parent.distance2Node(bidderNode,fareNode) < self._parent.distance2Node(winnerNode,fareNode):
                             allocatedTaxi = taxiIdx
                             winnerNode = bidderNode
                          else:
                             # and after all that, we still have to check that somebody won, because any of the other reasons to invalidate
                             # the auction may have occurred.




             #constraint 3
                             if allocatedTaxi >= 0:
                                # but if so, allocate the taxi.
                                self._fareBoard[origin][destination][time].taxi = allocatedTaxi     
                                self._parent.allocateFare(origin,self._taxis[allocatedTaxi])
                                winnerNode = bidderNode
     
