/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Vivek;

import static Vivek.BaseCloudletScheduler.vmparams;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;

/**
 *
 * @author Vivek Bindal
 */
public class Clustering {
    
    protected List<Cloudlet> Cloudletlist;
    protected List <Vm> vmlist;
    private   Map<Cloudlet, Double> rank;
    private Map<Integer, List<Cloudlet> > clusters;
    private Map<Cloudlet,Boolean> visited;
    public Clustering(List<Cloudlet> cloudletlist, List<Vm> vmlist,Map<Cloudlet, Double> rank) {
		// TODO Auto-generated constructor stub
               this.rank=rank;
               this.Cloudletlist = cloudletlist;
		this.vmlist = vmlist;
               clusters = new HashMap<Integer, List<Cloudlet>>();
               visited = new HashMap<Cloudlet,Boolean>();
               
    }
    public Map<Integer, List<Cloudlet> > getClusters()
    {
        return clusters;
    }
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
    private void sortranks(List<CloudletRank> cloudletRanklist) {
        /*for (Cloudlet cloudlet : rank.keySet()) {
            cloudletRanklist.add(new CloudletRank(cloudlet, rank.get(cloudlet)));
        }*/

     // Sorting in non-ascending order of rank
        Collections.sort(cloudletRanklist); 
    }
    public void dfs(){
        /*for(Object cloudletobj: Cloudletlist){   
         Cloudlet cl = (Cloudlet) cloudletobj;
         visited.put(cl, false);
        }*/	
        List<CloudletRank> cloudletRanklist = new ArrayList<CloudletRank>();
        for (Cloudlet cloudlet : rank.keySet()) {
            cloudletRanklist.add(new CloudletRank(cloudlet, rank.get(cloudlet)));
        }
        sortranks(cloudletRanklist);
        //Log.printLine("ranks sorted in dfs");
        /*while(visited.size()!=Cloudletlist.size()) {
            //allocateCloudletheft(cr.cloudlet);
            clusters.put(clusters.size()+1,new ArrayList<Cloudlet>());
        }*/
        for(CloudletRank cl:cloudletRanklist)
        {
           // clusters.put(clusters.size()+1,new ArrayList<Cloudlet>());
            if(!visited.containsKey(cl.cloudlet))
            {
                List<Cloudlet> ClusterList = new ArrayList<Cloudlet>();
		Log.printLine("NEW CLUSTER starts from"+cl.cloudlet.getCloudletId());	
                dfsutil(cl.cloudlet,ClusterList,visited);
                //Log.printLine("Number of clusters are "+clusters.size());
                clusters.put(clusters.size()+1, ClusterList);
            }
        }
    }
    private void dfsutil(Cloudlet cl ,List<Cloudlet> ClusterList,Map<Cloudlet,Boolean> visited)
    {
        ClusterList.add(cl);
        visited.put(cl,true);
        ArrayList<Cloudlet> children=Runner.getChildList(cl);
        /*for(Cloudlet child: children)
            System.out.print(child.getCloudletId()+"\t");
        */
        List<CloudletRank> cloudletRanklist = new ArrayList<CloudletRank>();
        for (Cloudlet cloudlet : children) {
        //    Log.printLine("Child is "+cloudlet.getCloudletId());
            cloudletRanklist.add(new CloudletRank(cloudlet, rank.get(cloudlet)));
        }
        
        sortranks(cloudletRanklist);
        //Log.printLine("Child sorted");
        /*for(CloudletRank child: cloudletRanklist)
            System.out.print(child.cloudlet.getCloudletId()+"\t");
        */
        for (CloudletRank rnk : cloudletRanklist) {
          //  Log.printLine("Child selected is "+rnk.cloudlet.getCloudletId());
            Boolean allParentScheduled=true;
            for (Cloudlet parent : Runner.getParentList(rnk.cloudlet))
            {
                if(!visited.containsKey(parent))
                {
                    allParentScheduled=false;
            //        Log.printLine("Parent "+parent.getCloudletId()+" not scheduled");
                    break;
                }
            }
            
            if(!visited.containsKey(rnk.cloudlet)&&allParentScheduled)
            {
                dfsutil(rnk.cloudlet,ClusterList,visited);
                break;
            }
            /*if(visited.containsKey(rnk.cloudlet))   
                Log.printLine("Already visited child");*/
        }
    }
                
            
	
}
