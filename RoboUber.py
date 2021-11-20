#   _________________________________________________________________
#   Comptuer Science: COMP6032
#   RoberUber Coursework
#   19129576
#   come219
#   __________________________________________________________________

#   _____________________________
#   Imported Packages
#   _____________________________
import pygame       # pygame for gui and other core components
import threading    # threading for application efficiency
import time         # standard time library
import math         # standard maths library
import numpy        # standard numpy library
import pandas       # standard pandas library

#   ______________________________________________________________
#   Python Modules that are used in conjunction with the project
#   the 3 Python modules containing the RoboUber objects
#   ______________________________________________________________
import networld     # environment which contains the streets and other map objects
import taxi         # the agents that traverse the map and picks up fares in the environment
import dispatcher   # the manager of the agents that assign bids to fares

#   ____________________________________
#   [ RoboUber Class ] : [networld Class], [Dipatcher Class] : [Taxi Class]
#   RoboUber contains the main function and initialises all objects from the pyscript to run solution
#   Defines declarations and returns main with system print output
#   ____________________________________

#   ___________________________________________________________________________
# TODO  (LEAGACY TODO COMMENT)
# create objects for RoboUber
#
# experiment with parameter settings. worldX and worldY should not need to be
# changed, but others are fair game!
# basic parameters
#   _______________________________________________________________


#   _______________________________________________________
#   World, Window and Traffic parameters
#   assign inital values of streets, fares, junctions
#   ______________________________________________________
worldX = 50                 # world's X parameter
worldY = 50                 # world's Y parameter
runTime = 1440              # total run time parameter
displaySize = (1024,768)    # you can change the DisplaySize to be bigger if you want larger-size objects on-screen
trafficOn = True            # Traffic parameter, causes problems in determining paths and creates the time destination to take longer

#   ______________________________________________________________________________________________
#   TODO (LEGACY COMMENT)
# play around with these parameters if you want, to see how they affect the results.
# (but keep the original settings so you can return to something more-or-less 'sensible)
#   _______________________________________________________________________________________________

#   _________________________________________________________________
#   Fare Parameters Assignment
#   Assigns different fare heurtistics, types
#   _______________________________________________________________
fareProbMagnet = lambda m: numpy.random.random() > 0.98         # most popular locations can generate a fare every hour
fareProbPopular = lambda p: numpy.random.random() > 0.992       # popular locations generate a fare about once every 2 hours
fareProbSemiPopular = lambda s: numpy.random.random() > 0.995   # semi-popular locations generate a fare approximately every 4 hours
fareProbNormal = lambda n: numpy.random.random() > 0.999        # normal locations generate a fare about once per day

#   _____________________________________________________________
#   Traffic Injector and Sinks Parameter Assignment
#   some traffic injectors and sinks for real-time simulation
#   ____________________________________________________________
trafficSrcMinor = 1 if trafficOn else 0
trafficSrcSignificant = 2 if trafficOn else 0
trafficSrcMajor = 3 if trafficOn else 0
trafficSrcHub = 4 if trafficOn else 0
trafficSinkMinor = 1 if trafficOn else 0
trafficSinkSignificant = 2 if trafficOn else 0
trafficSinkMajor = 3 if trafficOn else 0
trafficSinkDrain = 4 if trafficOn else 0

#   _______________________________________________________________________
#   Junction Node Parameter Assignment and Declaration
#   some nodes - this can be automated
#   __________________________________________________________________________
jct0 = networld.junctionDef(x=0, y=0, cap=2, canStop=True, src=trafficSrcMinor, sink=trafficSinkMinor)
jct1 = networld.junctionDef(x=20, y=0, cap=2, canStop=True, src=trafficSrcSignificant, sink=trafficSinkMinor)
jct2 = networld.junctionDef(x=40, y=0, cap=2, canStop=True, src=trafficSrcMajor, sink=trafficSinkMajor)
jct3 = networld.junctionDef(x=49, y=0, cap=2, canStop=True, src=trafficSrcMinor, sink=trafficSinkMinor)
jct4 = networld.junctionDef(x=0, y=10, cap=2, canStop=True, src=trafficSrcSignificant, sink=trafficSinkMinor)
jct5 = networld.junctionDef(x=10, y=10, cap=2, canStop=True, fareProb=fareProbSemiPopular, maxTraffic=12)
jct6 = networld.junctionDef(x=20, y=10, cap=2, canStop=True, maxTraffic=12)
jct7 = networld.junctionDef(x=24, y=15, cap=4, canStop=True, fareProb=fareProbSemiPopular, maxTraffic=12)
jct8 = networld.junctionDef(x=30, y=15, cap=4, canStop=True, fareProb=fareProbSemiPopular, maxTraffic=12)
jct9 = networld.junctionDef(x=40, y=15, cap=4, canStop=True, fareProb=fareProbPopular, maxTraffic=12)
jct10 = networld.junctionDef(x=49, y=15, cap=2, canStop=True, src=trafficSrcSignificant, sink=trafficSinkSignificant)
jct11 = networld.junctionDef(x=10, y=20, cap=2, canStop=True)
jct12 = networld.junctionDef(x=20, y=20, cap=4, canStop=True, fareProb=fareProbSemiPopular, maxTraffic=12)
jct13 = networld.junctionDef(x=10, y=24, cap=2, canStop=True)
jct14 = networld.junctionDef(x=20, y=24, cap=4, canStop=True)
jct15 = networld.junctionDef(x=24, y=24, cap=8, canStop=True, fareProb=fareProbMagnet, maxTraffic=16, src=trafficSrcHub, sink=trafficSinkMajor)
jct16 = networld.junctionDef(x=30, y=24, cap=4, canStop=True)
jct17 = networld.junctionDef(x=0, y=35, cap=2, canStop=True, src=trafficSrcSignificant, sink=trafficSinkMajor)
jct18 = networld.junctionDef(x=10, y=35, cap=4, canStop=True, fareProb=fareProbPopular, maxTraffic=12)
jct19 = networld.junctionDef(x=20, y=30, cap=4, canStop=True, fareProb=fareProbSemiPopular)
jct20 = networld.junctionDef(x=24, y=35, cap=4, canStop=True, fareProb=fareProbPopular, maxTraffic=12, src=trafficSrcMajor, sink=trafficSinkDrain)
jct21 = networld.junctionDef(x=30, y=30, cap=4, canStop=True)
jct22 = networld.junctionDef(x=40, y=30, cap=4, canStop=True, fareProb=fareProbSemiPopular, maxTraffic=12)
jct23 = networld.junctionDef(x=49, y=30, cap=2, canStop=True, src=trafficSrcMinor, sink=trafficSinkMinor)
jct24 = networld.junctionDef(x=10, y=40, cap=2, canStop=True)
jct25 = networld.junctionDef(x=15, y=40, cap=4, canStop=True, fareProb=fareProbPopular, maxTraffic=12)
jct26 = networld.junctionDef(x=30, y=40, cap=4, canStop=True, fareProb=fareProbSemiPopular, maxTraffic=12)
jct27 = networld.junctionDef(x=40, y=40, cap=2, canStop=True, maxTraffic=12)
jct28 = networld.junctionDef(x=0, y=49, cap=2, canStop=True, src=trafficSrcMinor, sink=trafficSinkMinor)
jct29 = networld.junctionDef(x=15, y=49, cap=2, canStop=True, src=trafficSrcSignificant, sink=trafficSinkMajor)
jct30 = networld.junctionDef(x=30, y=49, cap=2, canStop=True, src=trafficSrcMinor, sink=trafficSinkMinor)
jct31 = networld.junctionDef(x=49, y=49, cap=2, canStop=True, src=trafficSrcMinor, sink=trafficSinkMinor)

# Junction Array containing all the junctions
junctions = [jct0,jct1,jct2,jct3,jct4,jct5,jct6,jct7,jct8,jct9,jct10,jct11,jct12,jct13,jct14,jct15,
             jct16,jct17,jct18,jct19,jct20,jct21,jct22,jct23,jct24,jct25,jct26,jct27,jct28,jct29,jct30,jct31]
# Junction ID, returns array of node's X, Y
junctionIdxs = [(node.x,node.y) for node in junctions]


#   __________________________________________________________________
#   Street Nodes Parameters Assignment and Declaration
#   and some streets between them; likewise, this can be automated
#   _____________________________________________________________________
strt0 = networld.streetDef((0,0), (10,10), 3, 7, biDirectional=True)
strt1 = networld.streetDef((0,10),(10,10), 2, 6, biDirectional=True)
strt2 = networld.streetDef((0,35), (10,35), 2, 6, biDirectional=True)
strt3 = networld.streetDef((0,49), (10,40), 1, 5, biDirectional=True)
strt4 = networld.streetDef((10,10), (10,20), 4, 0, biDirectional=True)
strt5 = networld.streetDef((10,20), (10,24), 4, 0, biDirectional=True)
strt6 = networld.streetDef((10,24), (10,35), 4, 0, biDirectional=True)
strt7 = networld.streetDef((10,35), (10,40), 4, 0, biDirectional=True)
strt8 = networld.streetDef((10,10), (20,10), 2, 6, biDirectional=True)
strt9 = networld.streetDef((10,20), (20,20), 2, 6, biDirectional=True)
strt10 = networld.streetDef((10,24), (20,24), 2, 6, biDirectional=True)
strt11 = networld.streetDef((10,35), (20,30), 1, 5, biDirectional=True)
strt12 = networld.streetDef((10,35), (15,40), 3, 7, biDirectional=True)
strt13 = networld.streetDef((10,40), (15,40), 2, 6, biDirectional=True)
strt14 = networld.streetDef((20,0), (20,10), 4, 0, biDirectional=True)
strt15 = networld.streetDef((20,10), (20,20), 4, 0, biDirectional=True)
strt16 = networld.streetDef((20,20), (20,24), 4, 0, biDirectional=True)
strt17 = networld.streetDef((20,24), (20,30), 4, 0, biDirectional=True)
strt18 = networld.streetDef((15,40), (15,49), 4, 0, biDirectional=True)
strt19 = networld.streetDef((20,10), (24,15), 3, 7, biDirectional=True)
strt20 = networld.streetDef((20,20), (24,15), 1, 5, biDirectional=True)
strt21 = networld.streetDef((20,20), (24,24), 3, 7, biDirectional=True)
strt22 = networld.streetDef((20,24), (24,24), 2, 6, biDirectional=True)
strt23 = networld.streetDef((20,30), (24,24), 1, 5, biDirectional=True)
strt24 = networld.streetDef((20,30), (24,35), 3, 7, biDirectional=True)
strt25 = networld.streetDef((15,40), (24,35), 1, 5, biDirectional=True)
strt26 = networld.streetDef((15,40), (30,40), 2, 6, biDirectional=True)
strt27 = networld.streetDef((24,15), (24,24), 4, 0, biDirectional=True)
strt28 = networld.streetDef((24,24), (24,35), 4, 0, biDirectional=True)
strt29 = networld.streetDef((24,15), (30,15), 2, 6, biDirectional=True)
strt30 = networld.streetDef((24,24), (30,15), 1, 5, biDirectional=True)
strt31 = networld.streetDef((24,24), (30,24), 2, 6, biDirectional=True)
strt32 = networld.streetDef((24,24), (30,30), 3, 7, biDirectional=True)
strt33 = networld.streetDef((24,35), (30,30), 1, 5, biDirectional=True)
strt34 = networld.streetDef((24,35), (30,40), 3, 7, biDirectional=True)
strt35 = networld.streetDef((30,15), (30,24), 4, 0, biDirectional=True)
strt36 = networld.streetDef((30,24), (30,30), 4, 0, biDirectional=True)
strt37 = networld.streetDef((30,40), (30,49), 4, 0, biDirectional=True)
strt38 = networld.streetDef((30,15), (40,15), 2, 6, biDirectional=True)
strt39 = networld.streetDef((30,15), (40,30), 3, 7, biDirectional=True)
strt40 = networld.streetDef((30,40), (40,40), 2, 6, biDirectional=True)
strt41 = networld.streetDef((40,0), (40,15), 4, 0, biDirectional=True)
strt42 = networld.streetDef((40,15), (40,30), 4, 0, biDirectional=True)
strt43 = networld.streetDef((40,30), (40,40), 4, 0, biDirectional=True)
strt44 = networld.streetDef((40,15), (49,0), 1, 5, biDirectional=True)
strt45 = networld.streetDef((40,15), (49,15), 2, 6, biDirectional=True)
strt46 = networld.streetDef((40,30), (49,30), 2, 6, biDirectional=True)
strt47 = networld.streetDef((40,40), (49,49), 3, 7, biDirectional=True)

# Streets node array containing all the street nodes
streets = [strt0,strt1,strt2,strt3,strt4,strt5,strt6,strt7,strt8,strt9,strt10,strt11,strt12,strt13,strt14,strt15,
           strt16,strt17,strt18,strt19,strt20,strt21,strt22,strt23,strt24,strt25,strt26,strt27,strt28,strt29,strt30,strt31,
           strt32,strt33,strt34,strt35,strt36,strt37,strt38,strt39,strt40,strt41,strt42,strt43,strt44,strt45,strt46,strt47]

#   ________________________________________________________
#   Output Values Parameter Assignment and Declaration
#   create the dict of things we want to record
#   time, world simulation time
#   fares, array fares
#   taxis, array of taxis
#
#   Unimplemeted dict Output values
#   RevenueTaxis, total taxis revenue
#   RevenueDis, total dispatcher revenue
#   RevenueTotal, total revenue
#   Heuristic, heuristic parameter
#   _____________________________________________________________________
outputValues = {'time': [], 'fares': {}, 'taxis': {}}

#   ___________________________________________________________________________
#   Main Function: runRoboUber Function
#   RoboUber itself will be run as a separate thread for performance, so that screen
#   redraws aren't interfering with model updates.
#
#   Uses world X, Y paramenters, runTime Parameters, user stop parameter
#   Defines junctions, streets
#   Sets interpolate
#   Sets output values ->
#   **args
#   ___________________________________________________________________________________
def runRoboUber(worldX,worldY,runTime,stop,junctions=None,streets=None,interpolate=False,outputValues=None,**args):

   if 'fareProbNormal' not in args:     # Define Fare Probability in args
      args['fareProbNormal'] = lambda x: numpy.random.random() > 0.999

   print("Creating world...")           # create the NetWorld - the service area
   svcArea = networld.NetWorld(x=worldX,y=worldY,runtime=runTime,fareprob=args['fareProbNormal'],jctNodes=junctions,edges=streets,interpolateNodes=interpolate)

   print("Exporting map...")            # export the map to area (world)
   svcMap = svcArea.exportMap()

   if 'serviceMap' in args:             # assign service map in args
      args['serviceMap'] = svcMap

    #   _______________________________________________________________
    #   TODO (Old Output return code and graph)     ... set to remove
    #   link the graph to the taxis as they have their individual path
    #    graph = taxi.Graph()
    #    graph.edges = streets
    #    for edge in graph.edges:
    #        graph.add_edge(*edge)
    #    taxi0.edges = streets.
    #   __________________________________________________________________

#   _____________________________________________________________________________
#   Taxis function and Assignment
#   create some taxis and assign to array
#   ___________________________________________________________________________
   print("Creating taxis")
   taxi0 = taxi.Taxi(world=svcArea,taxi_num=100,service_area=svcMap,start_point=(20,0))
   taxi1 = taxi.Taxi(world=svcArea,taxi_num=101,service_area=svcMap,start_point=(49,15))
   taxi2 = taxi.Taxi(world=svcArea,taxi_num=102,service_area=svcMap,start_point=(15,49))
   taxi3 = taxi.Taxi(world=svcArea,taxi_num=103,service_area=svcMap,start_point=(0,35))

   taxis = [taxi0,taxi1,taxi2,taxi3]    # assign individual taxi X to taxis array

#   _______________________________________________________________________________
#   Dispatcher functino and Assignment
#   initialises dispatcher0 and assigns the world, svcArea and the taxis, taxis
#   ______________________________________________________________________________
   print("Adding a dispatcher")         # and a dispatcher
   dispatcher0 = dispatcher.Dispatcher(parent=svcArea,taxis=taxis)
   svcArea.addDispatcher(dispatcher0)   # who should be on duty

   print("Bringing taxis on duty")      # bring taxis to run
   for onDutyTaxi in taxis:             # bring the taxis on duty
       onDutyTaxi.comeOnDuty()

#   __________________________________________________________________________
#   Thread and Word Parameter
#   thread loop declaration and mechanic/method/law
#   ________________________________________________________________________
   threadRunTime = runTime              # thread run time, same as total run time
   threadTime = 0                       # thread running time init and declaration
   print("Starting world")              # start the world

   while threadTime < threadRunTime:    # thread loop


         if stop.is_set():              # exit if 'q' has been pressed
            threadRunTime = 0           # set thread run time to zero which causes application to close

         else:                          # run the thread
            svcArea.runWorld(ticks=1, outputs=outputValues)

            if threadTime != svcArea.simTime:

                threadTime += 1
                time.sleep(1)

userExit = threading.Event()                        # event to manage a user exit, invoked by pressing 'q' on the keyboard

roboUber = threading.Thread(target=runRoboUber,     #   RoboUber,thread of robouber with parameters
                            name='RoboUberThread',
                            kwargs={'worldX':worldX,
                                    'worldY':worldY,
                                    'runTime':runTime,
                                    'stop':userExit,
                                    'junctions':junctions,
                                    'streets':streets,
                                    'interpolate':True,
                                    'outputValues':outputValues,
                                    'fareProbMagnet':fareProbMagnet,
                                    'fareProbPopular':fareProbPopular,
                                    'fareProbSemiPopular':fareProbSemiPopular,
                                    'fareProbNormal':fareProbNormal})


#   ________________________________________________________________________
#   PyGame Function and Parameters
#
#   _________________________________________________________________________
pygame.init()                                                                       # init py game
displaySurface = pygame.display.set_mode(size=displaySize,flags=pygame.RESIZABLE)   # |pygame.SCALED arrgh...new in pygame 2.0, but pip install installs 1.9.6 on Ubuntu 16.04 LTS
backgroundRect = None                                                               # background rect
aspectRatio = worldX/worldY                                                         # aspect ratio of world x, world y
if aspectRatio > 4/3:                                                               # aspect ratio check
   activeSize = (displaySize[0]-100, (displaySize[0]-100)/aspectRatio)              # assign active size if greater than 4/3
else:
   activeSize = (aspectRatio*(displaySize[1]-100), displaySize[1]-100)              # assign 4/3 ratio
displayedBackground=pygame.Surface(activeSize)                                      # pygame surface display
displayedBackground.fill(pygame.Color(255,255,255))                                 # pygame color display
activeRect = pygame.Rect(round((displaySize[0]-activeSize[0])/2),round((displaySize[1]-activeSize[1])/2),activeSize[0],activeSize[1])
                                                                                    # pygame active rect
meshSize = ((activeSize[0]/worldX),round(activeSize[1]/worldY))                     # mesh size of active size

positions = [[pygame.Rect(round(x*meshSize[0]),             # create a mesh of possible drawing positions
                          round(y*meshSize[1]),
                          round(meshSize[0]),
                          round(meshSize[1]))
              for y in range(worldY)]
             for x in range(worldX)]
drawPositions = [[displayedBackground.subsurface(positions[x][y]) for y in range(worldY)] for x in range(worldX)]
                                                            # draw positions of surfaces and display

jctRect = pygame.Rect(round(meshSize[0]/4),                 # junctions exist only at labelled locations; it's convenient to create subsurfaces for them
                      round(meshSize[1]/4),
                      round(meshSize[0]/2),
                      round(meshSize[1]/2))
jctSquares = [drawPositions[jct[0]][jct[1]].subsurface(jctRect) for jct in junctionIdxs]
                                                            # junction squares of surfaces


for street in streets:                                      # initialise the network edge drawings (as grey lines)
    pygame.draw.aaline(displayedBackground,
                       pygame.Color(128,128,128),
                       (round(street.nodeA[0]*meshSize[0]+meshSize[0]/2),round(street.nodeA[1]*meshSize[1]+meshSize[1]/2)),
                       (round(street.nodeB[0]*meshSize[0]+meshSize[0]/2),round(street.nodeB[1]*meshSize[1]+meshSize[1]/2)))
    

for jct in range(len(junctionIdxs)):                        # initialise the junction drawings (as grey boxes)
    jctSquares[jct].fill(pygame.Color(192,192,192))         # note that the rectangle target in draw.rect refers to a Rect relative to the source surface, not an absolute-coordinates Rect.
    pygame.draw.rect(jctSquares[jct],pygame.Color(128,128,128),pygame.Rect(0,0,round(meshSize[0]/2),round(meshSize[1]/2)),5)
                                                            # pygame draw


displaySurface.blit(displayedBackground, activeRect)        # redraw the entire image   (re blit)
pygame.display.flip()                                       # display flip

taxiColours = {}                    # possible colours for taxis: black, blue, green, red, magenta, cyan, yellow, white

taxiPalette = [pygame.Color(0,0,0),             #color 0,0,0
               pygame.Color(0,0,255),           #color 0,0,255
               pygame.Color(0,255,0),
               pygame.Color(255,0,0),
               pygame.Color(255,0,255),
               pygame.Color(0,255,255),
               pygame.Color(255,255,0),
               pygame.Color(255,255,255)]

taxiRect = pygame.Rect(round(meshSize[0]/3),    # relative positions of taxi and fare markers in a mesh point
                       round(meshSize[1]/3),
                       round(meshSize[0]/3),
                       round(meshSize[1]/3))

fareRect = pygame.Rect(round(3*meshSize[0]/8),  # fare rect and draw
                       round(3*meshSize[1]/8),
                       round(meshSize[0]/4),
                       round(meshSize[1]/4))


curTime = 0             # curTime is the time point currently displayed

roboUber.start()            # start the simulation (which will automatically stop at the end of the run time)
while curTime < runTime:    # this is the display loop which updates the on-screen output.

      # End the simulation Function
      # Achieved by pressing 'q'.
      # This triggers an event which is also passed into the world loop
      try:
          quitevent = next(evt for evt in pygame.event.get() if evt.type == pygame.KEYDOWN and evt.key == pygame.K_q)
          userExit.set()
          pygame.quit()
          #sys.exit()

      except StopIteration: # event queue had no 'q' keyboard events. Continue.
          pygame.event.get()


          # print time function
          # returns the current time and time elpased from dict time
          if 'time' in outputValues and len(outputValues['time']) > 0 and curTime != outputValues['time'][-1]:
             print("curTime: {0}, world.time: {1}".format(curTime,outputValues['time'][-1]))    # prints out the cur time and the current world time



             # Print Fares Function
             # returns all the fares
             if 'fares' in outputValues and len(outputValues['fares']) > 0:
                 print("\n ___Fares Output___ \n")
                 farestext = pandas.DataFrame.from_dict(outputValues['fares'])
                 print(farestext)
                 print(" \n")

             # Print Taxis Function
             # returns all the taxis
             if 'taxis' in outputValues and len(outputValues['taxis']) > 0:
                print("\n ___Taxis Output___ \n")
                taxistext = pandas.DataFrame.from_dict(outputValues['taxis'])
                print(taxistext)
                print(" \n")

             # Print Revenue Function
             # returns all the revenue information
             if 'taxis' in outputValues and len(outputValues['taxis']) > 0:
                print("\n ___Revenues Output___ \n")

                print("NULL")
                print(" \n")
                print(" eot \n\n")

             # old time to output unformatted output of all stuff ... -> set to remove
             # print("Times: {0}, Fares: {1}, Taxis: {2}".format(outputValues['time'], outputValues['fares'].keys(), outputValues['taxis'].keys()))


             # naive: redraw the entire map each time step. This could be improved by saving a list of squares
             # to redraw and being incremental, but there is a fair amount of bookkeeping involved.
             displayedBackground.fill(pygame.Color(255,255,255))

             for street in streets:
                 pygame.draw.aaline(displayedBackground,
                                    pygame.Color(128,128,128),
                                    (round(street.nodeA[0]*meshSize[0]+meshSize[0]/2),round(street.nodeA[1]*meshSize[1]+meshSize[1]/2)),
                                    (round(street.nodeB[0]*meshSize[0]+meshSize[0]/2),round(street.nodeB[1]*meshSize[1]+meshSize[1]/2)))
    
             for jct in range(len(junctionIdxs)):
                 jctSquares[jct].fill(pygame.Color(192,192,192))
                 pygame.draw.rect(jctSquares[jct],pygame.Color(128,128,128),pygame.Rect(0,0,round(meshSize[0]/2),round(meshSize[1]/2)),5)
             
             # get fares and taxis that need to be redrawn. We find these by checking the recording dicts
             # for time points in advance of our current display timepoint. The nested comprehensions
             # look formidable, but are simply extracting members with a time stamp ahead of our
             # most recent display time. The odd indexing fare[1].keys()[-1] gets the last element
             # in the time sequence dictionary for a fare (or taxi), which, because of the way this
             # is recorded, is guaranteed to be the most recent entry.
             faresToRedraw = dict([(fare[0], dict([(time[0], time[1])
                                                   for time in fare[1].items()
                                                   if time[0] > curTime]))
                                   for fare in outputValues['fares'].items()
                                   if sorted(list(fare[1].keys()))[-1] > curTime])


             taxisToRedraw = dict([(taxi[0], dict([(taxiPos[0], taxiPos[1])
                                                   for taxiPos in taxi[1].items()
                                                   if taxiPos[0] > curTime]))
                                   for taxi in outputValues['taxis'].items()
                                   if sorted(list(taxi[1].keys()))[-1] > curTime])

             # some taxis are on duty?
             if len(taxisToRedraw) > 0:
                for taxi in taxisToRedraw.items():
                    # new ones should be assigned a colour
                    if taxi[0] not in taxiColours and len(taxiPalette) > 0:
                       taxiColours[taxi[0]] = taxiPalette.pop(0)
                    # but only plot taxis up to the palette limit (which can be easily extended)
                    if taxi[0] in taxiColours:
                       newestTime = sorted(list(taxi[1].keys()))[-1]
                       # a taxi shows up as a circle in its colour
                       pygame.draw.circle(drawPositions[taxi[1][newestTime][0]][taxi[1][newestTime][1]],
                                          taxiColours[taxi[0]],
                                          (round(meshSize[0]/2),round(meshSize[1]/2)),
                                          round(meshSize[0]/3))

                   
             # some fares still awaiting a taxi?
             if len(faresToRedraw) > 0:
                for fare in faresToRedraw.items():
                    newestFareTime = sorted(list(fare[1].keys()))[-1]
                    # fares are plotted as orange triangles (using pygame's points representation which
                    # is relative to the rectangular surface on which you are drawing)
                    pygame.draw.polygon(drawPositions[fare[0][0]][fare[0][1]],
                                        pygame.Color(255,128,0),
                                        [(meshSize[0]/2,meshSize[1]/4),
                                         (meshSize[0]/2-math.cos(math.pi/6)*meshSize[1]/4,meshSize[1]/2+math.sin(math.pi/6)*meshSize[1]/4),
                                         (meshSize[0]/2+math.cos(math.pi/6)*meshSize[1]/4,meshSize[1]/2+math.sin(math.pi/6)*meshSize[1]/4)])
                   
             # redraw the whole map 
             displaySurface.blit(displayedBackground, activeRect)
             pygame.display.flip()

             # advance the time                           
             curTime += 1






      
