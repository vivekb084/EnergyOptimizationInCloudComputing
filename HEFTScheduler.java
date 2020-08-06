//@Nidhi Rehani, nidhirehani@gmail.com, NIT Kurukshetra

package Vivek;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
//import org.cloudbus.cloudsim.BaseCloudletScheduler.Event;


public class HEFTScheduler extends BaseCloudletScheduler{
	
    private   Map<Cloudlet, Double> rank;
    protected Map<Vm, List<Event>> originalschedules;
    protected Map<Cloudlet, Double> earliestFinishTimes;
    
    protected Map<Cloudlet, Double> shiftamount;
    private Map<Integer, List<Cloudlet> > clusters;
    private Map<List<Cloudlet>,Vm> minimum_energy_vm;
    
    int countfinal =0;//to specify the position of failure slot to check for the chosenvm
    int countcurrent =0;//to specify the position of failure slot to check for the curent vm
    //int maxfr =160;
   // int numberoffailures =0;
    //double begin[], end[];
    int seq=0;
    
    protected class CloudletRank implements Comparable<CloudletRank> {

        public Cloudlet cloudlet;
        public Double rank;

        public CloudletRank(Cloudlet cloudlet, Double rank) {
            this.cloudlet = cloudlet;
            this.rank = rank;
        }

        @Override
        public int compareTo(CloudletRank o) {
            return o.rank.compareTo(rank);
        }
    }

	public HEFTScheduler(List<Cloudlet> cloudletlist, List<Vm> vmlist) {
		// TODO Auto-generated constructor stub
		super(cloudletlist, vmlist);
                minimum_energy_vm=new HashMap<List<Cloudlet>,Vm>();
		rank = new HashMap<Cloudlet, Double>();
        earliestFinishTimes = new HashMap<Cloudlet, Double>();
        originalschedules = new HashMap<Vm, List<Event>>();
        schedules = new HashMap<Vm, List<Event>>();
        clusters = new HashMap<Integer, List<Cloudlet>>();
               
        //  shiftamount = new HashMap<>();
        
		
	}
	
	public void run(){
		try{
			File heftfile = new File("HEFTResults");
			BufferedWriter HEFTWriter = new BufferedWriter(new FileWriter(heftfile, true));
			
			Log.printLine("HEFT scheduler with MCS running with " + Cloudletlist.size()
	                + " cloudlets.");

	        averageBandwidth = calc_avg_bw();

	        for (Object vmObject : vmlist) {
	            Vm vm = (Vm) vmObject;
	            originalschedules.put(vm, new ArrayList<Event>());
	            schedules.put(vm, new ArrayList<Event>());
	        }
	        
	        
	        // Prioritization phase
	        calc_ComputationCosts();
	        calc_TransferCosts();
	        calculateRanks();
                /*for(Cloudlet cl:rank.keySet())
                {
                    Log.printLine("Cloudlet "+cl.getCloudletId()+"\t"+rank.get(cl));
                }*/
                //Log.printLine("Ranks calculated");
                Clustering ClusterObj =new Clustering(Cloudletlist,vmlist,rank);
                ClusterObj.dfs();
                clusters=ClusterObj.getClusters();
                                    Log.printLine(clusters.size());
                allocateCloudletsheft(clusters);
               /* for(Integer i : clusters.keySet())
                {                                    
                    Log.print("Cluster number "+i+"\t");
                    List<Cloudlet> CloudletList=clusters.get(i);
                    for(int j=0;j<CloudletList.size();j++)
                    {
                       Log.print(CloudletList.get(j).getCloudletId()+"\t");
                    }
                    Log.printLine();
                }
                */
               
              // Map<List<Cloudlet>,Map<Vm,Double> > clus_vm_exec ;
               //clus_vm_exec=new HashMap <List<Cloudlet>,Map<Vm,Double> >();
               /*Map <List<Cloudlet>,Map<Vm,Double>> clus_vm_exec;
               clus_vm_exec=new HashMap <List<Cloudlet>,Map<Vm,Double>>(); 
                for(Vm i:vmlist)
                {
                     Vm vm = (Vm) i;
			double executionEnergy = 0.0;
			double executionTime = 0.0;
                    for(Integer j:clusters.keySet())
                    {
                        double temp=0;
                        List <Cloudlet> CloudletList=clusters.get(j);
                        for(Cloudlet clobject: CloudletList)
                        {
                            temp=clobject.getCloudletLength();
                            executionEnergy += findEnergyConsumption(vm, temp);
                        }
			Map<Vm, Double> vd = new HashMap<Vm, Double>();
                        vd.put(vm, executionEnergy);
                        clus_vm_exec.put(CloudletList,vd);
                    }
                }
                */
              // calcu_min_energy_vm(clus_vm_exec) ;
               
		
		
	        /*Log.printLine("Ranks calculated");
	      //  allocatevmavailability();
			allocateVmPowerParameters();
	        // Selection phase
	        Log.printLine("Allocation for HEFT");
	        allocateCloudletsheft();
	        
	        for (Object cloudletObject : Cloudletlist) {
	            Cloudlet cloudlet = (Cloudlet) cloudletObject;
	            shiftamount.put(cloudlet, 0.0);
	        }
	        /*
	        System.out.println("Original HEFT schedule");
	        for(int i =0; i<Cloudletlist.size(); i++){
	        	System.out.print("Cloudlet " + i + " : start : " + begin[i] + " finish : " + end[i] );
	        	System.out.println();
	        }
	        
	        System.out.println("\nafter adjusting heft schedule for failures");
	        for(Cloudlet cloudletObject: Cloudletlist){
	        	Cloudlet cloudlet = cloudletObject;
	        	int i = cloudlet.getCloudletId();
	        	System.out.print("Cloudlet: " + i + " vm allocated: " + cloudlet.getVmId() + "  : start : " + begin[i] + " finish : " + end[i] );
	            startTimes.put(cloudlet, begin[i]);
	            durationTimes.put(cloudlet, end[i]);
	            reservationIds.put(cloudlet, cloudlet.getVmId());
	        	System.out.println();
	        }
	        System.out.println();
	        //set schedules for each vm after failure adjustment
	        for (Object vmObject : vmlist) {
	            Vm vm = (Vm) vmObject;
	            List<Event> sched = originalschedules.get(vm);
	            List<Event> newsched = schedules.get(vm);
	            for(Object event: sched){
	            	Event ev = (Event) event;
	            	int id = ev.cloudlet.getCloudletId();
	            	newsched.add(new Event(begin[id], end[id], ev.cloudlet, vm.getMips()));
	            }
	        }
	        findExecutionEnergyConsumption();
	        totalEnergyConsumption = findTotalEnergyConsumption();
	        System.out.println("\nThe Total Energy Consumption is: " + totalEnergyConsumption);
	        System.out.println("Number of failures occured: " + numberoffailures);
	        //write the makespan for heft in the specified file
	        makespan = end[Cloudletlist.size()-1] ;
	        HEFTWriter.write(makespan + "\t" + totalEnergyConsumption + "\n" );
	        HEFTWriter.close();*/
		}catch(Exception e){
			e.printStackTrace();
		}
	}
        public void calcu_min_energy_vm(Map <List<Cloudlet>,Map<Vm,Double>> clus_vm_exec)
        {
            for(List<Cloudlet> cluster:clus_vm_exec.keySet())
            {
                Map<Vm,Double> vms=new HashMap<Vm,Double>();
                        vms=clus_vm_exec.get(cluster);
                double min_energy=Double.MAX_VALUE;
                Vm assigned_vm=null;
                for(Vm vm:vms.keySet())
                        {
                            double energy=vms.get(vm);
                            if(energy<min_energy)
                            {
                                min_energy=energy;
                                assigned_vm=vm;
                            }
                        }
                minimum_energy_vm.put(cluster,assigned_vm);
            }
        }
	public void calculateRanks() {
		for (Object cloudletObject : Cloudletlist) {
			Cloudlet cloudlet = (Cloudlet) cloudletObject;
			calculateRank(cloudlet);
		}
	}

	public double calculateRank(Cloudlet cloudlet) {
		if (rank.containsKey(cloudlet)) {
			return rank.get(cloudlet);
		}

		for (Double cost : computationCosts.get(cloudlet).values()) {
			averageComputationCost += cost;
		}

		averageComputationCost /= computationCosts.get(cloudlet).size();

		double max = 0.0;
		for (Cloudlet child : Runner.getChildList(cloudlet)) {
			double rankval = calculateRank(child);
			double childCost = transferCosts.get(cloudlet).get(child)+ rankval;
			max = Math.max(max, childCost);
		}

		rank.put(cloudlet, averageComputationCost + max);
		//print the rank for the cloudlet
		//System.out.println("Cloudlet: " + cloudlet.getCloudletId() + "rank: " + rank.get(cloudlet));
		return rank.get(cloudlet);
	}
	
    private void allocateCloudletsheft(Map<Integer, List<Cloudlet>> clusters) {
        

     // Sorting in non-ascending order of rank
        //Collections.sort(cloudletRank);
        for (Integer cluster : clusters.keySet()) {
            Log.printLine("Cluster nO"+cluster);
            allocateCloudletheft(clusters.get(cluster));
        }
    }
    
    private void allocateCloudletheft(List<Cloudlet> cluster) {
    	//sequence[seq] = cloudlet.getCloudletId(); 
    	seq++;
        Vm chosenvm = null;
        double earliestFinishTime = Double.MAX_VALUE;
        double bestReadyTime = 0.0;
        double finishTime;

        for (Vm vmObject : vmlist) {
            Log.printLine("\tVm id is\t"+vmObject.getId());
            Vm vm = (Vm) vmObject;
            double minReadyTime = 0.0;

           /* for( clobj:cluster)
            {
            for (Cloudlet parent : Runner.getParentList(clobj)) {
                double readyTime = earliestFinishTimes.get(parent);
                if (parent.getVmId() != vm.getId()) {
                    readyTime += transferCosts.get(parent).get(clobj);
                }

                minReadyTime = Math.max(minReadyTime, readyTime);
            }
                            finishTime = findFinishTime(cloudlet, vm, minReadyTime, false);

            }*/
            List <Event> Schedules =new ArrayList<Event>();
            findFinishTime(cluster, vm,Schedules);
            finishTime=Schedules.get(Schedules.size()-1).finish;
            if (finishTime < earliestFinishTime) {
                bestReadyTime = minReadyTime;
                earliestFinishTime = finishTime;
                chosenvm = vm;
            }
        }
            List <Event> Schedules =new ArrayList<Event>();
            findFinishTime(cluster, chosenvm,Schedules);
            originalschedules.put(chosenvm, Schedules);
        //findFinishTime(cloudlet, chosenvm, bestReadyTime, true);
                        Log.printLine("\tVmschosen\t");

        for(Event e:Schedules)
        {
            int id=e.cloudlet.getCloudletId();
            if(cluster.contains(id))
            {
                begin[id]=e.start;
                end[id]=e.finish;
                vmallocated[id]=chosenvm.getId();
                earliestFinishTimes.put(e.cloudlet, e.finish);
                e.cloudlet.setVmId(chosenvm.getId());
                Log.printLine("Cluster\t"+ id+"   Allocated on VM\t"+chosenvm.getId());
            }
        }
       
        
    }
    
/*
    private void allocateCloudletheft(Cloudlet cloudlet) {
    	//sequence[seq] = cloudlet.getCloudletId(); 
    	seq++;
        Vm chosenvm = null;
        double earliestFinishTime = Double.MAX_VALUE;
        double bestReadyTime = 0.0;
        double finishTime;

        for (Object vmObject : vmlist) {
            Vm vm = (Vm) vmObject;
            double minReadyTime = 0.0;

            for (Cloudlet parent : Runner.getParentList(cloudlet)) {
                double readyTime = earliestFinishTimes.get(parent);
                if (parent.getVmId() != vm.getId()) {
                    readyTime += transferCosts.get(parent).get(cloudlet);
                }

                minReadyTime = Math.max(minReadyTime, readyTime);
            }

            finishTime = findFinishTime(cloudlet, vm, minReadyTime, false);

            if (finishTime < earliestFinishTime) {
                bestReadyTime = minReadyTime;
                earliestFinishTime = finishTime;
                chosenvm = vm;
            }
        }

        findFinishTime(cloudlet, chosenvm, bestReadyTime, true);

        earliestFinishTimes.put(cloudlet, earliestFinishTime);

        cloudlet.setVmId(chosenvm.getId());
    }*/
    
    private void findFinishTime(List<Cloudlet> cluster, Vm vm, List<Event> Schedule) {
    	//List<Event> Schedule=new ArrayList<Event>();
        Schedule=originalschedules.get(vm);
        int vid = vm.getId();
        //List<Event> sched = originalschedules.get(vm);
        double finishtime=0.0;
         Map<Cloudlet, Double> FinishTimes=new HashMap<Cloudlet, Double>();
         FinishTimes=earliestFinishTimes;
        for(Cloudlet cloudlet:cluster)
        {
            Log.printLine("\t\tCloudlet No "+cloudlet.getCloudletId());
           
            double minReadyTime=0.0;
            for (Cloudlet parent : Runner.getParentList(cloudlet)) {
                Log.printLine("\t\t\tPareni is "+parent.getCloudletId());
                
                double readyTime = FinishTimes.get(parent);
                                Log.printLine("World");

                if (parent.getVmId() != vm.getId()) {
                    readyTime += transferCosts.get(parent).get(cloudlet);
                }

                minReadyTime = Math.max(minReadyTime, readyTime);
            }
                            //finishTime = findFinishTime(cloudlet, vm, minReadyTime, false);
            int id = cloudlet.getCloudletId();
            double computationCost = computationCosts.get(cloudlet).get(vm);
            double start, finish;
            int pos;

            if (Schedule.size() == 0) {
                
                Schedule.add(new Event(minReadyTime, minReadyTime + computationCost, cloudlet, vm.getMips()));
                /*if (occupySlot) {
                    originalschedules.add(new Event(minReadyTime, minReadyTime + computationCost, cloudlet, vm.getMips()));
                    begin[id] = readyTime;
                    end[id] = readyTime + computationCost;
                //vmallocated[id] = vid;
                    System.out.println("Cloudlet: " + id + "\tVm allocated: " + vid + "  begin : " + begin[id]  + "  end:  " + end[id]);
                }*/
                 
                //return minReadyTime + computationCost;
            }

        if (Schedule.size() == 1) {
            if (minReadyTime >= Schedule.get(0).finish) {
                pos = 1;
                start = minReadyTime;
            } else if (minReadyTime + computationCost <= Schedule.get(0).start) {
                pos = 0;
                start = minReadyTime;
            } else {
                pos = 1;
                start = Schedule.get(0).finish;
            }
            Schedule.add(pos, new Event(start, start + computationCost, cloudlet, vm.getMips()));
                
/*            if (occupySlot) {
                sched.add(pos, new Event(start, start + computationCost, cloudlet, vm.getMips()));
                begin[id] = start;
                end[id] = start + computationCost;
                //vmallocated[id] = vid;
                System.out.println("Cloudlet: " + id + "\tVm allocated: " + vid + "  begin : " + begin[id]  + "  end:  " + end[id]);
            }*/
           // return start + computationCost;

            }
        // Trivial case: Start after the latest task scheduled
        start = Math.max(minReadyTime, Schedule.get(Schedule.size() - 1).finish);
        finish = start + computationCost;
        int i = Schedule.size() - 1;
        int j = Schedule.size() - 2;
        pos = i + 1;
        while (j >= 0) {
            Event current = Schedule.get(i);
            Event previous = Schedule.get(j);

            if (minReadyTime > previous.finish) {
                if (minReadyTime + computationCost <= current.start) {
                    start = minReadyTime;
                    finish = minReadyTime + computationCost;
                }

                break;
            }

            if (previous.finish + computationCost <= current.start) {
                start = previous.finish;
                finish = previous.finish + computationCost;
                pos = i;
            }

            i--;
            j--;
        }

        if (minReadyTime + computationCost <= Schedule.get(0).start) {
            pos = 0;
            start = minReadyTime;
            Schedule.add(pos, new Event(start, start + computationCost, cloudlet, vm.getMips()));
               
            /*if (occupySlot) {
                Schedule.add(pos, new Event(start, start + computationCost, cloudlet, vm.getMips()));
                begin[id] = start;
                end[id] = start + computationCost;
               // vmallocated[id] = vid;
                System.out.println("Cloudlet: " + id + "\tVm allocated: " + vid + "  begin : " + begin[id]  + "  end:  " + end[id]);
            }
            */
           // return start + computationCost;
        }
        /*if (occupySlot) {
            Schedule.add(pos, new Event(start, finish, cloudlet, vm.getMips()));
            begin[id] = start;
            end[id] = start + computationCost;
            //vmallocated[id] = vid;
            System.out.println("Cloudlet: " + id + "\tVm allocated: " + vid + "  begin : " + begin[id]  + "  end:  " + end[id]);
        }*/
        //return finish;
        }
}
    
   
    
    private void adjustcloudletfailure(Cloudlet cloudlet, double totalshift){
    	
    	for (Cloudlet child : Runner.getChildList(cloudlet)) {
				double prevshift = shiftamount.get(child);
				double difference =0;
				//shift time if total shift is more than already shifted amount
				if(totalshift > prevshift){
					difference = totalshift - prevshift;
					begin[child.getCloudletId()] = begin[child.getCloudletId()] + difference;
		    		end[child.getCloudletId()] = end[child.getCloudletId()] + difference;
		    		shiftamount.put(child, totalshift);
		    		//System.out.println("\nCloudlet id: " + child.getCloudletId() + "  previous shift: " + prevshift + " total shift now: " + shiftamount.get(child) );
				}
				adjustcloudletfailure(child, totalshift);
			}
    }


}
