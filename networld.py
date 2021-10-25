import math
import numpy
import heapq

from taxi import Taxi

# some straightforward data containers to help in initialising Worlds. A junction will
# end up being a Node, a street will end up being an Edge.

# a junctionDef contains:
# 1) its (x,y) coordinates
# 2) how many taxis can occupy the junction
# 3) whether a taxi is permitted to stop here
# 4) a function to set the probability of a Fare appearing. This should be a
# callable object that returns a value; it can thus be anything from a simple
# lambda function to an arbitrary lookup table.
# 5) how much traffic the Node can hold before locking
# 6) traffic that 'appears' in the node (usually, at edges of the graph)
# 7) traffic that 'disappears' from the node (usually, at the edges of the graph)
class junctionDef:

      def __init__(self, x, y, cap, canStop, fareProb=None, maxTraffic=0, src=0, sink=0):
          self.x = x
          self.y = y
          self.capacity = cap
          self.canStop = canStop
          self.fareProb = fareProb
          self.maxTraffic = maxTraffic
          self.tSrc = src
          self.tSink = sink

# a streetDef contains:
# 1) the ID of the 2 end nodes
# 2) the expected 'outward' connection direction for both (where 'outward' describes
# the direction that would be relevant if leaving the node via that edge were possible),
# 3) whether the edge is bidirectional.
class streetDef:
      def __init__(self, nodeAIdx, nodeBIdx, dirA, dirB, biDirectional=True):
          self.nodeA = nodeAIdx # first point (origin if one-way)
          self.nodeB = nodeBIdx # second pont (destination if one-way
          self.dirA = dirA      # exit point from nodeA
          self.dirB = dirB      # exit point from nodeB
          self.bidirectional = biDirectional # one-way or 2-way street?

'''
A Fare is a very simple object residing in the world, representing a passenger to be delivered to some
destination starting from some origin. Fares wait for a taxi for a finite amount of time, given by 
maxWait, which is not known to the dispatcher or taxis, then abandon their request. Fares 'live' in a
queue ordered by abandon time, which is popped at each time step and sent to the dispatcher as a cancelled
fare. Meanwhile, they likewise appear at their origin nodes at random times - an event also given to the
dispatcher. They have a few simple methods which do the more or less obvious things: collecting and
dropping them off and assigning a price.
'''
class Fare:

      def __init__(self, parent, origin, destination, call_time, wait_time):

         self._parent = parent
         self._origin = origin
         self._destination = destination
         self._callTime = call_time
         self._waitTime = wait_time
         self._taxi = None
         self._enroute = False
         self._price = 0

      # board a taxi
      def pickUp(self, taxi):
          if self._taxi == taxi:
             self._enroute = True
             self._parent.removeFare(self)

      # alight from a taxi
      def dropOff(self):
          self._enroute = False
          self._origin = self._destination

      # inform the fare of the expected price for the ride
      def setPrice(self, price):
          self._price = price
          # abandon the call if the asked-for price is exorbitant (in this case, if
          # it exceeds 10x the expected travel time), or if the situation on the
          # ground is so gridlocked that no one is getting anywhere, any time soon.
          expectedTime2Dest = self._parent.travelTime(self._origin, self._destination)
          if (expectedTime2Dest < 0) or (self._price > 10*expectedTime2Dest):
             print("Fare ({0},{1}) abandoned because expectedTime2Dest was {2} and price was {3}".format(self.origin[0],self.origin[1],expectedTime2Dest, self._price))
             self._waitTime = 0

      # clear gets rid of any references to objects so that garbage collection can
      # delete the object for sure.
      def clear(self):
          self._parent = None
          self._origin = None
          self._destination = None
          self._taxi = None

      def assignTaxi(self, taxi):
          self._taxi = taxi

      @property
      def enroute(self):
          return self._enroute

      @property
      def origin(self):
          return self._origin.index

      @property
      def destination(self):
          return self._destination.index

      @property
      def calltime(self):
          return self._callTime

      @property
      def maxWait(self):
          return self._waitTime

      @property
      def price(self):
          return self._price

      @property
      def taxi(self):
          return self._taxi
'''
A Node represents any reachable point in the RoboUber world. Nodes can be thought of as lying on a
square grid with all the cardinal compass points being adjacent - i.e. the diagonals as well as 
the sides are accessible. A Node has a certain maximum occupancy (in terms of taxis). Any taxis
above the occupancy limit must wait until a space in the Node has been vacated. We allow more
than one occupant at some points to prevent there being a bottleneck at major junctions (nodes
with high in-degree). For simplicity we require that only one taxi per direction can ever be
present in a Node.
To simulate traffic, there are several flow control properties and methods built into a Node. 
Taxis must attempt to access a Node using indicate(). This signals intent to occupy the Node.
Taxis can abandon the attempt at any time using free(). When capacity becomes available, the
Node uses the traffic_light property to signal which taxi in the waiting list gains access.
Taxis must poll the traffic_light to see if they can enter, which they then do by using
occupy(). A taxi leaves a node using vacate() (which will, of course, only be possible if
the node it's moving to has space). 
There is also a traffic property, that the taxi can interrogate, which is a simple representation
of the overall (i.e. non-taxi) traffic in the Node. The amount of traffic in the node indicates
how many time steps are necessary to vacate the Node. If traffic reaches 5 (gridlock), the taxi
cannot vacate until traffic drops below 5.
There is a stop_allowed property. Fares can only be present at a node if stop is allowed. There
will only ever be one fare at a given stop (to minimise management of fare allocation). A taxi
can always opt simply to stop at a given location, but if stop is not allowed, it must not.

'''          
class Node:

      # class constructor. Most arguments are optional, but a Node must have an index
      # (a location) and a parent (a world it 'lives' in.
      
      def __init__(self,parent,index,can_stop=True,capacity=1,
                   fare_probability=None,traffic_cap=0,traffic_in=0,traffic_out=0,
                   N=None,S=None,E=None,W=None,NE=None,NW=None,SE=None,SW=None):

          # initialise neighbours in an adjacency list. This allows relatively simple computation
          # of turns; just compute the next positive or negative index, wrapping around.
          self._idx = index                        # index is the node's (x,y) tuple in the network
          self._neighbours = [N,NE,E,SE,S,SW,W,NW] # reachable neighbours (not necessarily symmetric)
          self._canStop = can_stop                 # taxis can stop here
          self._capacity = capacity                # max number of taxis that can be in this point 
          self._occupied = {}                      # dictionary of taxis at this point, indexed by current direction
          self._incoming = {}                      # dictionary of taxis attempting to enter this point
          self._traffic_light = 0                  # priority management for access. Indexes the direction with first priority
          self._trafficMax = traffic_cap           # _traffic point where the Node becomes locked
          self._trafficSrc = traffic_in            # amount of traffic automatically generated coming in per clock
          self._trafficSink = -traffic_out         # amount of traffic automatically removed per clock
          self._traffic = 0
          self._fare = None
          self._fare_generator = fare_probability
          self._parent = parent

          # default the traffic capacity to a level of 8 (thus a junction can accept an input from all
          # sides before becoming overloaded)
          if self._trafficMax == 0:            
             self._trafficMax = 8
          # default fare generator just randomly produces a new fare with probability given by the size of the network
          if self._fare_generator is None:
             if self._parent.size == 0:
                # 1/1000 chance if there is no information on network size (approximately 1 call per day)
                self._fare_generator = lambda t: numpy.random.random() > 0.999
             else:
                # otherwise default probability would generate on average 1 call every 100 minutes for the service area
                self._fare_generator = lambda q: numpy.random.random() > 1 - 1/(10*self._parent.size())

      # properties of the Node that other objects can see
      
      @property
      def canStop(self):
          return self._canStop                
                
      @property
      def capacity(self):
          return self._capacity

      @property
      def haveSpace(self):
          return self._traffic < self._trafficMax

      @property
      def index(self):
          return self._idx

      @property
      def maxTraffic(self):
          return self._trafficMax
    
      # neighbours returns a list of non-None adjacent Nodes (i.e. accessible locations from
      # this Node) as tuples of the format (index, x, y) where index is the absolute directional
      # index in the _neighbours list
      @property
      def neighbours(self):
          return [(neighbour,
                   self._neighbours[neighbour].index[0],
                   self._neighbours[neighbour].index[1])
                  for neighbour in range(len(self._neighbours))
                  if self._neighbours[neighbour] is not None]
          
      @property
      def occupied(self):
          return len(self._occupied)

      @property
      def traffic(self):
          return self._traffic

      # methods generally called by the parent world

      # adds an adjoining node. Used in building the graph
      def addNeighbour(self, parent, neighbour, direction):
          if self._parent == parent:
             self._neighbours[direction] = neighbour

      ''' clockTick handles the core functionality of a node at each timestep. The function
          should be called by the Node's parent; there is a check that the calling object is
          indeed the parent. Within the timer tick the following things happen: taxis in the 
          incoming list are scheduled for access; traffic is flowed through (if traffic in 
          neighbouring Nodes is not blocking); and fares appear or disappear (the latter if
          it has been waiting too long)
      '''     
      def clockTick(self, parent):
          if self._parent == parent:
             # flow through traffic first, to give the node the best chance of allowing taxis in
             for neighbour in self._neighbours:
                 if neighbour is not None and neighbour.haveSpace and self._traffic > 0:
                    parent.addTraffic(neighbour)
                    self._traffic -= 1
                    self.injectTraffic(self._parent,self._trafficSink)
             # off-duty taxis which were here should be removed. Build a new list so
             # as not to do odd things in the _occupied dict
             offDuty = [taxi for taxi in self._occupied.items() if not taxi[1][0].onDuty]
             for taxi in offDuty:
                 del self._occupied[taxi[0]]
             # now deal with admitting taxis
             if len(self._incoming) > 0 and self._traffic < self._trafficMax and len(self._occupied) < self._capacity:
                remaining = self._capacity - len(self._occupied)
                admitted = {}
                while remaining > 0 and len(admitted) < len(self._incoming):
                      # spin the traffic light until a taxi is found. This will
                      # admit taxis in fair round-robin fashion
                      while self._traffic_light not in self._incoming:
                            self._traffic_light = (self._traffic_light+1) % 8
                      admitted[self._traffic_light] = self._incoming[self._traffic_light]
                      remaining -= 1
                parent.issueAdmission(self, admitted)
             # next deal with new fares appearing or abandoning
             if self._fare is not None:
                # fares won't wait forever for a ride
                if parent.simTime-self._fare.calltime > self._fare.maxWait:
                   parent.removeFare(self._fare)
                   self._fare = None
             # and will only hail a taxi in allowed stopping locations
             elif self._canStop:
                if self._fare_generator(self._parent.simTime):
                   self._fare = parent.insertFare(self)
             # last thing to do is inject intrinsic traffic
             self.injectTraffic(self._parent,self._trafficSrc)

      # add some traffic into the node
      def injectTraffic(self, parent, volume):
          if self._parent == parent:
             if self._traffic > self._trafficMax:
                return 0
             self._traffic += volume
             if self._traffic > self._trafficMax:
                excess = self._traffic - self._trafficMax
                self._traffic = self._trafficMax
                return volume-excess
             return volume

      # methods generally called by Taxis. Almost all of these are automated into the Taxi
      # machinery.

      ''' 
          ____________________________________________________________________________________________________________
          These methods allow the taxi to move between Nodes using drive. There are 3 steps.
          First, the taxi makes a decision to turn or continue straight through (if it can).
          Then, it must indicate to the Node that it is turning (and it can abandon the attempt
          if it waited too long).
          Finally, it vacates its old Node and occupies the new one.
      '''
              
      ''' turn requests an index for an adjacent node. This automatically indicates
          the direction change to the neighbour, and returns an (index, neighbour)
          tuple to the taxi showing which neighbour it should interrogate the traffic
          light for, and which direction the traffic light needs to show before it
          can access the node. 
          Direction indicates the relative egress point. 0 proceeds straight ahead, if this is possible.
          Positive directions index the nth possible left. Negative directions index the -nth
          possible right. A direction of None simply leaves the space without informing any
          adjacent space - this can be used e.g. to take down road works or other obstructions.
      '''
      def turn(self, directionIn, directionOut=-1):
          # if traffic is enabled, don't allow the taxi out of the space until a delay equivalent to the
          # traffic volume has elapsed. A traffic value of trafficMax indicates gridlock! In addition when the
          # taxi attempts to occupy a Node, the world controller will assign it a travel time to the
          # Node from the one it vacated. This means that even if traffic is disabled (self._traffic = 0)
          # the taxi takes finite time to traverse the Node.
          if self._traffic == self._trafficMax or self._parent.simTime-self._occupied[directionIn][1] < self._traffic:
             return (None, -1)
          relativeDirection = directionOut
          # default to straight ahead
          if relativeDirection  < 0:
             relativeDirection = (directionIn + 4) % 8
             # if directly straight ahead is not possible, the 2 diagonals could be sensible
             # provided only one is available. If both are available, we have an ambiguous fork
             # in the road.
             if self._neighbours[relativeDirection] is None:
                relativeDirection = (relativeDirection - 1) % 8
                alternativeDirection = (relativeDirection + 2) % 8
                if self._neighbours[relativeDirection] is None:
                   relativeDirection = alternativeDirection
                   if self._neighbours[relativeDirection] is None:
                      return (None, -1)
                elif self._neighbours[alternativeDirection] is not None:
                   return (None, -1)
          # otherwise the outward direction is indexed as appropriate
          # illegal direction; U-turns not allowed here or too few possible lefts/rights
          if self._neighbours[relativeDirection] is None:
             return (None, -1)     
          # otherwise indicate to the neighbour. In its world, incoming will be opposite outgoing
          newDirection = (relativeDirection + 4) % 8
          self._neighbours[relativeDirection].indicate(newDirection,self._occupied[directionIn][0])
          # and return what the new direction will be in the adjacent node, along with the node itself
          return (self._neighbours[relativeDirection], newDirection)

      # convenience function to avoid having taxis call the turn() method (which would be confusing in
      # this context) if they intend to proceed through straight ahead. This will fail if there is
      # no sensible 'straight ahead' direction.
      def continueThrough(self, directionIn):
          return self.turn(directionIn)

      # indicate requests access to the Node. direction is the incoming direction
      # as seen from the Node.
      def indicate(self, direction, occupant):
          self._incoming[direction] = occupant

      # abandon turns off an existing indication (e.g. if the taxi waited too long to gain admission)    
      def abandon(self, direction, occupant):
          if self._incoming[direction] == occupant:
             del self._incoming[direction]

      # claims the space. A vehicle can only occupy an available space.
      def occupy(self, direction, occupant, origin=None):
          # occupying a space takes finite time; it doesn't happen instantaneously.
          time2Occupy = self._parent.travelTime(origin,self)
          # all sorts of ways occupancy can fail: too many taxis already here; a taxi
          # has already taken the desired direction, the would-be occupant didn't
          # signal, a rogue taxi tried to swerve around the proper occupant, or this
          # node is backed up with traffic.
          if (len(self._occupied) == self._capacity or direction in self._occupied or
              direction not in self._incoming or self._incoming[direction] != occupant
              or time2Occupy < 0):
             print ("Taxi {0} can't occupy node: full".format(occupant.number))
             return (None, -1)
          self._occupied[direction] = (occupant,self._parent.simTime+time2Occupy)
          del self._incoming[direction]
          self._parent.clearAdmission(self,occupant)
          return (self, direction)

      # leaves the space if the occupant can. Returns the space occupied after the attempt
      # to vacate (which can fail). directionIn indexes which taxi is leaving. directionOut
      # is the index given by the target space that the taxi will be in when it enters the
      # space
      def vacate(self, directionIn, directionOut=None):
          # automatically vacate if there is no outward direction
          if directionOut is None:
             del self._occupied[directionIn]
             return (None, -1)
          # direction as seen from this Node is the diametric opposite of that seen
          # at the adjacent node
          relativeDirection = (directionOut + 4) % 8
          # occupy the neighbouring space if possible
          newSpace = self._neighbours[relativeDirection].occupy(directionOut,self._occupied[directionIn][0],self)
          if newSpace[0] is None and newSpace[1] == -1:
             return (self, directionIn)
          del self._occupied[directionIn]
          return newSpace

      '''--------------------------------------------------------------------------------------------------------
         These next methods deal with collecting and dropping off Fares.
      '''

      # taxis call this function to pick up the fare. The fare's pickUp routine will register
      # with the world controller that it has been collected by the calling taxi. A fare may
      # have since abandoned, in which case the attempt to pick up will fail. Otherwise a
      # taxi in the right location will get the fare returned to it.
      def pickupFare(self, direction=None):
          if direction in self._occupied:
             fare = self._fare
             if fare is not None and fare.taxi == self._occupied[direction][0]:
                self._fare = None
                fare.pickUp(self._occupied[direction][0])
                return fare
          return None
             
      # taxis call this function to drop off the fare. This will notify the world controller
      # that the fare has been conducted to the destination (and the controller will allocate
      # payments accordingly)
      def dropoffFare(self, fare, direction):
          if direction in self._occupied:
             # fares will only alight at their actual destination (naturally)
             if fare is not None and fare.destination == self._idx and self._occupied[direction][1] >= self._parent.simTime:
                fare.dropOff()
                self._parent.completeFare(fare)
                return True
          return False

'''
NetWorld is the main class responsible for driving the simulation. It contains the road network
graph, the time-stepper, and the controller that deals with taxi and dispatcher commands. Notionally,
a time step is a minute of 'real time', but it could, in principle, be almost anything. The 
graph is a map of Nodes, each of which lies at some (x,y) coordinate.
'''
    
class NetWorld:

      '''
      Constructor. The x, y size of the world must be specified, all other parameters
      are optional:
      runtime - number of  time steps in the simulation (0 = run forever)
      fareprob - A default fare probability generator can be specified, this should
      be a Python callable object similar to the Node's fare probability generator
      jctNodes - a list of junctionDefs specifying the nodes of the graph
      edges = a list of streetDefs specifying the edges of the graph
      interpolateNodes - if True, the constructor will infer default nodes between each
      junction point for all x,y coordinates in the path between them; this will allow taxis
      to stop at any point on a street and fares to appear there. If not, only the nodes themselves
      will be generated and this means taxis can only stop at junctions and fares will
      only ever appear there.
     '''
      def __init__(self,x,y,runtime = 0, fareprob=None, jctNodes=None,edges=None,interpolateNodes=False):

          # size of the virtual grid. Nodes must be at (x,y) positions within the grid.
          self.xSize = x
          self.ySize = y
          # number of time steps to run. 0 means run forever.
          self.runTime = runtime
          # default fare generator is used for interpolated positions. A defined Node can
          # have its own fare probability structure
          self.defaultFareGen = fareprob
          # the network itself (which starts blank) is a dictionary indexed by node number
          # (a straightforward (x,y) hash)
          self._net = {}
          # the traffic queue is a dictionary of entries for each node into which traffic is
          # to be injected
          self._trafficQ = {}
          # the taxi queue is a dictionary of entries for each taxi giving (node, direction)
          # admission token pairs
          self._taxis = {}
          # this is a dict indexed by origin of the active fares waiting for collection
          self._fareQ = {}
          # the dispatcher (there can only be one) handles allocation of fares to taxis
          self._dispatcher = None
          if jctNodes is not None:
             self.addNodes(jctNodes)
          if edges is not None:
             self.addEdges(edges,interpolateNodes)
          #self.eventQ = NetEventQueue()
          # the simulation clock. 
          self._time = 0

      # properties

      # simTime gives the current time tick
      @property
      def simTime(self):
          return self._time

      # only one property of the world is automatically available, the size of the world in number
      # of nodes.
      @property
      def size(self):
          return len(self._net)

      #__________________________________________________________________________________________________________
      # methods to build the graph and place agents in it

      # takes a list of junctionDefs and adds it to the network as Nodes. Any nodes will overwrite
      # existing Nodes at the same location. This does not affect incoming links, but it will clear
      # affect outgoing ones, so the expectation is that addNodes will be followed by an addEdges
      # that reinstates any outgoing edges from this (junction) node.
      def addNodes(self, nodes):

          self._net.update([((node.x,node.y),
                             Node(**{'parent': self,
                                     'index': (node.x,node.y),
                                     'can_stop': node.canStop,
                                     'capacity': node.capacity,
                                     'fare_probability': node.fareProb,
                                     'traffic_cap': node.maxTraffic,
                                     'traffic_in': node.tSrc,
                                     'traffic_out': node.tSink}))
                            for node in nodes])

      # takes a list of streetDefs and adds it to the network as outgoing links. Interpolate
      # will create a series of interstitial nodes from the source to the destination. 
      def addEdges(self, edges, interpolate=False):

          # in the interpolation case, we create additional interstitial nodes between junctions
          if interpolate:
             for edge in edges:
                 if edge.nodeA not in self._net:
                    raise ValueError("Node {0} does not exist in the map".format(edge.nodeA))
                 if edge.nodeB not in self._net:
                    raise ValueError("Node {0} does not exist in the map".format(edge.nodeB))
                 # extract source and destination-adjacent nodes, because we might exit each node
                 # along a slightly different angle than the direct origin-destination path
                 src = self._net[edge.nodeA]
                 dst = self._net[edge.nodeB]
                 srcExitx = src.index[0]
                 srcExity = src.index[1]
                 dstExitx = dst.index[0]
                 dstExity = dst.index[1]
                 if edge.dirA < 2 or edge.dirA > 6:
                    if src.index[1] <= dst.index[1]:
                       raise ValueError("Road exit point from node {0} points away from destination node {1}".format(src.index, dst.index))
                    srcExity -= 1
                 if edge.dirA > 0 and edge.dirA < 4:
                    if src.index[0] >= dst.index[0]:
                       raise ValueError("Road exit point from node {0} points away from destination node {1}".format(src.index, dst.index))    
                    srcExitx += 1
                 if edge.dirA > 4:
                    if src.index[0] <= dst.index[0]:
                       raise ValueError("Road exit point from node {0} points away from destination node {1}".format(src.index, dst.index))
                    srcExitx -= 1
                 if edge.dirA > 2 and edge.dirA < 6:
                    if src.index[1] >= dst.index[1]:
                       raise ValueError("Road exit point from node {0} points away from destination node {1}".format(src.index, dst.index))
                    srcExity += 1
                 if edge.dirB < 2 or edge.dirB > 6:
                    if dst.index[1] <= src.index[1]:
                       raise ValueError("Road exit point from node {0} points away from destination node {1}".format(dst.index, src.index))  
                    dstExity -= 1
                 if edge.dirB > 0 and edge.dirB < 4:
                    if dst.index[0] >= src.index[0]:
                       raise ValueError("Road exit point from node {0} points away from destination node {1}".format(dst.index, src.index))  
                    dstExitx += 1
                 if edge.dirB > 4:
                    if dst.index[0] <= src.index[0]:
                       raise ValueError("Road exit point from node {0} points away from destination node {1}".format(dst.index, src.index))  
                    dstExitx -= 1
                 if edge.dirB > 2 and edge.dirB < 6:
                    if dst.index[1] >= src.index[1]:
                       raise ValueError("Road exit point from node {0} points away from destination node {1}".format(dst.index, src.index))  
                    dstExity += 1
                 nextNodeIdx = (srcExitx,srcExity)
                 # the trivial case where origin and destination are already adjacent can just be dealt with immediately
                 if nextNodeIdx[0] == dst.index[0] and nextNodeIdx[1] == dst.index[1]:
                    if self.addEdgeSegment(src, nextNodeIdx,edge.bidirectional) != dst:
                       raise KeyError("Indexed node ({0},{1}) should be the end junction {2}".format(nextNodeIdx[0],nextNodeIdx[1],dst.index))
                    return
                 # in other cases, note that the penultimate node may not exist. In the non-bidirectional case, this means we
                 # have to add it before stepping through the rest of the interpolated nodes, because again, its angle to the
                 # end node might be different than the straight-line origin-destination angle.
                 penultimateNodeIdx = (dstExitx,dstExity)
                 if not edge.bidirectional:
                    if penultimateNodeIdx not in self._net:
                       penultimateNode = Node(**{'parent': self,
                                                 'index': penultimateNodeIdx,
                                                 'can_stop': True,
                                                 'capacity': 1,
                                                 'fare_probability': self.defaultFareGen})
                       self._net[penultimateNodeIdx] = penultimateNode
                    if self.addEdgeSegment(penultimateNode,(dst.index[0], dst.index[1]),False) != dst:
                       raise KeyError("Penultimate node {1} should be adjacent to end junction {2}".format(penultimateNode.index,dst.index))
                 # bidirectional cases can be just handled by the ordinary machinery.
                 else :
                       penultimateNode = self.addEdgeSegment(dst,penultimateNodeIdx,edge.bidirectional)
                 # insert the adjacent node from the origin
                 nextNode = self.addEdgeSegment(src, (srcExitx,srcExity), bidir=True)
                 # and then finally, step through between these 2 penultimate nodes to create the complete edge
                 while nextNode != penultimateNode:
                       nextNode = self.addEdgeSegment(nextNode, penultimateNodeIdx, bidir=True)

          # when we don't have to interpolate, the process is a LOT simpler                           
          else:
             for edge in edges:
                 self._net[edge.nodeA].addNeighbour(self, self._net[edge.nodeB], edge.dirA)
                 if edge.bidirectional:
                    self._net[edge.nodeB].addNeighbour(self, self._net[edge.nodeA], edge.dirB)

      # builds edges by adding one segment at a time, auto-computing entry and exit points.
      # start is the actual point from which we are going to add another segment. We assume
      # the start Node exists. end is NOT necessarily the next node, it is the ultimate endpoint
      # of this edge.
      def addEdgeSegment(self, start, end, bidir=True):
          # compute the position of the next point
          deltaX = end[0]-start.index[0]
          deltaY = end[1]-start.index[1]
          deltaTheta = math.atan2(deltaY, deltaX)
          xStep = round(math.cos(deltaTheta))
          yStep = round(math.sin(deltaTheta))
          nextIdx = (start.index[0]+xStep, start.index[1]+yStep)
          # which must not be out of the area of the network
          if nextIdx[0] > self.xSize-1 or nextIdx[0] < 0 or nextIdx[1] > self.ySize-1 or nextIdx[1] < 0:
             raise IndexError("Next node out of range: ({0},{1})".format(nextIdx[0], nextIdx[1]))
          # add it if it doesn't exist
          if nextIdx not in self._net:
             nextNode = Node(**{'parent': self,
                                'index': nextIdx,
                                'can_stop': True,
                                'capacity': 2 if bidir else 1,
                                'fare_probability': self.defaultFareGen})
             self._net[nextIdx] = nextNode
          # find the exit directions from each node 
          startEgress = 0
          backEgress = 4
          if xStep > 0:
             if yStep > 0:
                startEgress = 3  
                backEgress = 7
             elif yStep < 0:
                startEgress = 1
                backEgress = 5
             else:
                startEgress = 2
                backEgress = 6
          elif xStep < 0:
             if yStep > 0:
                startEgress = 5
                backEgress = 1
             elif yStep < 0:
                startEgress = 7
                backEgress = 3
             else:
                startEgress = 6
                backEgress = 2
          else:
              if yStep > 0:
                 startEgress = 4
                 backEgress = 0
              else:
                 startEgress = 0
                 backEgress = 4
          # then add the links between nodes as necessary
          start.addNeighbour(self, self._net[nextIdx], startEgress)
          if bidir:
             self._net[nextIdx].addNeighbour(self, start, backEgress)
          return self._net[nextIdx]

      # places a taxi. Taxis can only come in at the boundaries of the service area, through established
      # roads. Like all other traffic, they have to gain admission to a Node; they're not automatically
      # allowed in
      def addTaxi(self, taxi, location):
          # taxis can only come in on the edges, and only if they're on duty
          if not taxi.onDuty:
             return (None, -1)
          ingressPoint = -1
          if location[0] == 0:
             if location[1] == 0:
                self._net[location].indicate(7,taxi)
                ingressPoint = 7
             elif location[1] == self.ySize-1:
                self._net[location].indicate(5,taxi)
                ingressPoint = 5
             else:
                self._net[location].indicate(6,taxi)
                ingressPoint = 6
          elif location[0] == self.xSize-1:
             if location[1] == 0:
                 self._net[location].indicate(1,taxi)
                 ingressPoint = 1
             elif location[1] == self.ySize-1:
                 self._net[location].indicate(3,taxi)
                 ingressPoint = 3
             else:
                 self._net[location].indicate(2,taxi)
                 ingressPoint = 2
          elif location[1] == 0:
             self._net[location].indicate(0,taxi)
             ingressPoint = 0
          elif location[1] == self.ySize-1:
             self._net[location].indicate(4,taxi)
             ingressPoint = 4
          else:
             return (None, -1)
          # the dispatcher ought to know a new taxi is available
          if self._dispatcher is not None:
             self._dispatcher.addTaxi(taxi)
          # a taxi just coming on duty has no predefined right of way
          self._taxis[taxi] = (None, -1)
          # but does get a traffic light indicator to allow it in
          return (self._net[location],ingressPoint)

      # adds a dispatcher. This is straightforward because the dispatcher doesn't need to have any physical location.
      # a dispatcher added using this method will supersede any previous dispatchers that have been there.
      def addDispatcher(self,dispatcher):
          # place the dispatcher in the world
          self._dispatcher = dispatcher
          # let it know the taxis that are already there,
          for taxi in self._taxis.keys():
              self._dispatcher.addTaxi(taxi)
          # give it the map of the service area,
          self._dispatcher.importMap(self.exportMap())
          # and any fares that happen to be waiting already
          for fare in self._fareQ.values():
              # we can also handle fares previously dispatched by another dispatcher
              if fare.taxi is not None:
                 self._dispatcher.handover(self, fare.origin, fare.destination, fare.callTime, fare.taxi, fare.price)
              else:
                 self._dispatcher.newFare(self, fare.origin, fare.destination, fare.callTime)

      #----------------------------------------------------------------------------------------------------------------

      #informational methods agents can use to interrogate various aspects of the world

      # accesses the node given an x,y coordinate
      def getNode(self, x, y):
          if (x,y) not in self._net:
             return None
          return self._net[(x,y)]

      ''' this dumps out the complete map of the network. it is arranged as a nested dictionary. The
          outer dict is indexed by each node's (x,y) coordinate and there is one entry per node. Its
          inner dict is a map of the nodes to which the outer node connects directly, indexed by the
          destination (x,y) coordinate and giving a tuple of outward direction from the origin to the
          destination along with the distance to it.
      ''' 
      def exportMap(self):
          return dict([(node.index,
                        dict([((neighbour[1],neighbour[2]),
                               (neighbour[0], self.distance2Node(node,self._net[(neighbour[1],neighbour[2])])))
                              for neighbour in node.neighbours]))
                        for node in self._net.values()])

      # the next 2 functions are heuristics, that is, in some situations they will not be strictly
      # accurate. 
      # travel time between 2 nodes. If the nodes are directly connected this
      # will be an exact heuristic.
      def travelTime(self, origin, destination):
          # a None value from either node indicates coming in/going into 'The Void'.
          # regardless of situation, a taxi can always 'evaporate' into the void
          # effectively instantly
          if destination is None:
             return 0
          # but a taxi cannot simply materialise into a node; if the node is bunged
          # up with traffic, it will have to wait, potentially indefinitely
          if origin is None:
             if destination.traffic == destination.maxTraffic:
                return -1
             else:
                return 0
          # a travel time of -1 indicates an indefinite wait into the future. We are, in other words,
          # not getting anywhere anytime soon.
          if origin.traffic == origin.maxTraffic or destination.traffic == destination.maxTraffic:
             return -1
          # all other destinations take finite time to reach.
          else:
             return round((origin.traffic+destination.traffic+self.distance2Node(origin, destination))/2)

      # straight-line distance between 2 nodes. If the nodes are directly connected
      # this will be an exact heuristic
      def distance2Node(self, origin, destination):
          # give an invalid distance if the nodes were invalid
          if origin is None or destination is None:
             return -1
          return math.sqrt((destination.index[0]-origin.index[0])**2+(destination.index[1]-origin.index[1])**2)

      #_____________________________________________________________________________________________________________
      # methods called by world's members to execute coordinated actions
      
      '''methods generally called by Nodes
      '''
      
      # addTraffic is called by a Node and inserts traffic into another Node. We can
      # do this 'properly' on an event queue if desired
      def addTraffic(self, node):
          if node.index in self._trafficQ:
             self._trafficQ[node.index] +=1
          else:
             self._trafficQ[node.index] = 1
    
      # issueAdmission is called by a Node and sends OK-to-enter messages to Taxis.
      # admissionList is a dictionary of (direction, taxi) entries that give a
      # taxi the direction token needed to enter the Node.
      def issueAdmission(self, node, admissionList):
          for taxi in admissionList.items():
              if taxi[1] not in self._taxis:
                 raise ValueError("Node {0} tried to admit a taxi that does not exist".format(node.index))
              # punish greedy taxis that try to enter multiple nodes by denying them entry
              # to all. They'll soon learn not to try to hedge their bets...
              if (self._taxis[taxi[1]] != (None, -1) or not taxi[1].onDuty):
                 self._taxis[taxi[1]] = (None, -1)
              # otherwise admit any on-duty taxis
              else:
                 self._taxis[taxi[1]] = (node, taxi[0])

      # clearAdmission is called by a Node to release a taxi's admission when it has entered
      # the node.
      # --BUGFIX -- added 6 December 2020 
      def clearAdmission(self, node, taxi):
          if taxi in self._taxis and self._taxis[taxi][0] == node:
             self._taxis[taxi] = (None, -1)

      # insertFare is called by a Node, creates a fare, adds it to the Node, and notifies
      # the Dispatcher
      def insertFare(self, node):
          if node.index in self._fareQ:
             raise IndexError("Node {0} generated a new fare for one where a Fare is already waiting".format(node.index))
          # generate a random valid destination. This isn't actually ideal, what we really want is a distance-dependent
          # distribution over the node pairs, but implementing a *fast* generator of such a form is not easy. Dictionaries,
          # furthermore, have in Python 3.x the subtlety that their values() can't be indexed, it returns an iterator
          # instead, so you have to turn it into a list. It may actually be faster to grab the keys(), turn those into
          # a list, then index the element in self._net.
          destinationNode = node
          while destinationNode == node or not destinationNode.canStop:
                destinationNode = list(self._net.values())[round(numpy.random.uniform(0,len(self._net)-1))]
          # fares will wait only for so long; a function of the distance to destination plus a gamma distribution 
          maxWait = self.distance2Node(node, destinationNode)*10 + 5*numpy.random.gamma(2.0,1.0)
          newFare = Fare(self, node, destinationNode, self._time, maxWait)
          # notify the Dispatcher, if any. If there is no Dispatcher yet, when it does come on-shift, it will
          # be notified of any pending Fares that it ought to dispatch, assuming they've not abandoned the attempt.
          # Dispatchers get no idea of how long a fare will wait! 
          if self._dispatcher is not None:
             self._dispatcher.newFare(self, newFare.origin, newFare.destination, newFare.calltime)
          self._fareQ[newFare.origin] = newFare
          return newFare

      def removeFare(self, fare):
          # if the fare wasn't collected, inform the dispatcher that they abandoned
          if not fare.enroute:
             if self._dispatcher is not None:
                self._dispatcher.cancelFare(self,
                                            fare.origin,
                                            fare.destination,
                                            fare.calltime)
          # both collected and abandoned fares disappear from the fare queue
          del self._fareQ[fare.origin]

      '''methods generally called by Dispatchers
      '''

      # broadcastFare is called by the Dispatcher, and issues a message to Taxis informing
      # them of a fare from origin to destination with a given price
      def broadcastFare(self, origin, destination, price):
          # small chance the fare has already given up, if the Dispatcher took too long to get around
          # to announcing it. If so, inform the Dispatcher
          if origin not in self._fareQ or self._fareQ[origin].destination != destination:
             return 0
          # let the fare know the price
          self._fareQ[origin].setPrice(price)
          onDuty = 0
          # inform the taxis,
          for taxi in self._taxis.keys():
              if taxi.onDuty:
                 onDuty +=1
                 taxi.recvMsg(taxi.FARE_ADVICE, **{'origin': origin, 'destination': destination, 'price': price})
          # and return how many taxis were advised
          return onDuty

      # allocateFare is called by the Dispatcher, and assigns a given fare to a given Taxi.
      def allocateFare(self, origin, taxi):
          # fare may have abandoned the attempt, while the taxi may have gone off-duty
          if origin not in self._fareQ or taxi not in self._taxis or not taxi.onDuty:
             return False
          self._fareQ[origin].assignTaxi(taxi)
          taxi.recvMsg(taxi.FARE_ALLOC, **{'origin': origin, 'destination': self._fareQ[origin].destination})

                      
      # cancelFare is called by the Dispatcher when a fare abandons their request, and informs any allocated
      # taxi so that they don't need to chase a nonexistent fare. This will also help taxis to estimate
      # fare-abandonment rates.
      def cancelFare(self, origin, taxi):
          if taxi not in self._taxis:
             return False
          # dispatchers *should* have a reference to the taxis they have allocated, and *should* check that
          # they're not off. But just in case, to prevent any spurious messages being sent...
          if taxi.onDuty:
             taxi.recvMsg(taxi.FARE_CANCEL, **{'origin': origin})

      '''methods generally called by Taxis
      '''

      # completeFare is called by the taxi and registers the fare as conducted to the destination
      # which then assigns the fare's price to the Taxi and Dispatcher.
      def completeFare(self, fare):
          self._dispatcher.recvPayment(self,fare.price*0.1)
          fare.taxi.recvMsg(fare.taxi.FARE_PAY, **{'amount': fare.price*0.9})
          # get rid of the fare's taxi allocation so that garbage collection doesn't have to worry
          # about back pointers. The taxi itself should already have got rid of the fare in the
          # completeFare call.
          #print("fare paid: " + fare.price*0.9)
          fare.clear()

      # transmitFareBid is called by the taxi and notifies the Dispatcher that it it is willing
      # to accept the fare. It also notifies the Dispatcher of the taxi's location.
      def transmitFareBid(self, origin, taxi):
          self._dispatcher.fareBid(origin, taxi)

      #----------------------------------------------------------------------------------------------------------------
                 
      # runWorld operates the model. It can be run in single-stepping mode (ticks = 1), batch mode
      # (ticks = 0) or any number of step-aggregation modes (ticks > 1). In batch mode, live output
      # may be unreliable, if running in a separate thread.
      def runWorld(self,ticks=0,outputs=None):
          if outputs is None:
             outputs = {}
          ticksRun = 0
          while (ticks == 0 or ticksRun < ticks) and (self.runTime == 0 or self._time < self.runTime):
                print("Current time in the simulation world: {0}".format(self._time))
                if 'time' in outputs:
                   outputs['time'].append(self._time)
                # really simple recording of fares: just where there are fares still waiting. More
                # sophisticated recording including price information and enroute fare information,
                # could easily be added, e.g. by making the fare output a list of fare objects.
                if 'fares' in outputs:
                   for fare in self._fareQ.values():
                       if fare.origin in outputs['fares']:
                          outputs['fares'][fare.origin][self._time] = fare.calltime
                       else:
                          outputs['fares'][fare.origin] = {self._time: fare.calltime}
                # go through all the nodes and update the time tick
                for node in self._net.values():
                    node.clockTick(self)
                    # we can output live traffic information if we want. Or possibly other
                    # parameters of a node, depending on how much reporting is desirable. (With
                    # very large networks and lots of reporting, this could slow things considerably)                   
                    if 'nodes' in outputs:
                        if node.index in outputs['nodes']:
                           outputs['nodes'][node.index][self._time] = node.traffic
                        else:
                           outputs['nodes'][node.index] = {self._time: node.traffic}
                # next go through the (live) taxis
                for taxi in self._taxis.items():
                    if taxi[0].onDuty:
                       taxi[0].drive(taxi[1])
                       taxi[0].clockTick(self)
                       # similarly basic recording of taxis: just their current position, as long as they
                       # are on duty. 
                       if 'taxis' in outputs:
                          if taxi[0].number in outputs['taxis']:
                             outputs['taxis'][taxi[0].number][self._time] = taxi[0].currentLocation
                          else:
                             outputs['taxis'][taxi[0].number] = {self._time: taxi[0].currentLocation}
                    # an off-duty taxi can come on if it decides to (and will call addTaxi to add itself)
                    else:
                       taxi[0].comeOnDuty(self._time)
                # then run the dispatcher. With this ordering, taxis bidding for fares can always get
                # them allocated immediately (provided the dispatcher decides to do so). Taxis always
                # receive notice of potential fares for collection one clock after the fare first appeared
                # to the dispatcher. We can make this fully asynchronous if we wish with an event queue.
                if self._dispatcher is not None:
                   self._dispatcher.clockTick(self)
                # new traffic arrives last. Since we flow old traffic out of Nodes first, this gives
                # taxis the best chance to reach a Node, they shouldn't be helplessly stuck whilst
                # traffic flows around them.
                for node in self._trafficQ.items():
                    self._trafficQ[node[0]] -= self._net[node[0]].injectTraffic(self, node[1])
                # update the batch stepper
                self._time += 1
                ticksRun +=1                

 
