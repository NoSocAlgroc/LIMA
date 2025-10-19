package edu.upc.data.lattice;

import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Predicate;

import com.koloboke.collect.map.IntObjCursor;
import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.koloboke.collect.map.hash.HashObjObjMap;
import com.koloboke.collect.map.hash.HashObjObjMaps;


public class Lattice {


    public class Node {


        public HashIntObjMap<Edge[]> to;
        public HashIntObjMap<Edge> from;
        public HashIntIntMap preds;

        public Node base;


        public Node(HashIntIntMap preds) {
            this.preds=HashIntIntMaps.newImmutableMap(preds);
            this.from=HashIntObjMaps.newUpdatableMap();
            this.to=HashIntObjMaps.newUpdatableMap();
        }

        @Override
        public String toString() {
            return this.preds.toString();
        }

    }


    public class Edge {

        public Edge base;

        public Node from,to;
        public int cp,p;
        public Edge(Node from, int cp, int p, Node to) {
            this.cp=cp;
            this.from=from;
            this.p=p;
            this.to=to;
        }
        @Override
        public String toString() {
            return this.from.preds.toString()+" + ("+Integer.toString(this.cp)+"="+Integer.toString(this.p)+")";
        }
    }





    public int[] preds;

    public HashObjObjMap<HashIntIntMap,Node> nodes;

    public Node root;

    public Lattice base;

    public Lattice(int[] preds) {
        this.preds=preds;
        this.nodes=HashObjObjMaps.newUpdatableMap();
        this.root=new Node(HashIntIntMaps.newImmutableMap(HashIntIntMaps.newUpdatableMap()));
        this.base=this;
    }

    public Lattice(Lattice base) {
        this.base=base;
        this.preds=base.preds;
        this.nodes=HashObjObjMaps.newUpdatableMap();
        this.root=new Node(HashIntIntMaps.newImmutableMap(HashIntIntMaps.newUpdatableMap()));
    }


    public Node fetchRoot() {
        return this.fetchNode(HashIntIntMaps.newMutableMap());
    }

    public Node fetchNode(HashIntIntMap preds) {
        Node n=this.nodes.get(preds);
        if(n==null) {
            if(preds.size()==0)n=this.root;
            else {
                n=new Node(preds);                
            }
            this.nodes.put(preds, n);
            n.base=this.base==this?n:this.base.fetchNode(preds);
        }
        return n;
    }


    public Edge getTo(Node n, int cp, int p) {
        Edge[] ecps=n.to.get(cp);
        if(ecps!=null) return ecps[p];
        return null;
    }
    public Edge getFrom(Node n, int cp, int p) {
        Edge e=n.from.get(cp);
        if(e!=null) assert e.p==p;
        return e;
    }

    Edge[] fetchToEdges(Node n, int cp) {
        Edge[] ecps=n.to.get(cp);
        assert !n.preds.containsKey(cp);
        if(ecps==null)
        {
            ecps=new Edge[Lattice.this.preds[cp]];
            n.to.put(cp, ecps);
        }
        return ecps;
    }
    public Edge fetchTo(Node n, int cp, int p) {
        Edge[] ecps=this.fetchToEdges(n, cp);
        Edge e=ecps[p];
        if(e==null) {
            HashIntIntMap newPreds=HashIntIntMaps.newUpdatableMap(n.preds);
            assert !newPreds.containsKey(cp);
            newPreds.addValue(cp, p);
            Node toNode=this.fetchNode(newPreds);
            e=new Edge(n, cp, p, toNode);
            linkEdge(e);
            e.base=this.base==this?e:this.base.fetchTo(this.base.fetchNode(e.from.preds),e.cp,e.p);
            
        }
        return e;
    }
    public Edge fetchFrom(Node n, int cp, int p) {
        Edge e=n.from.get(cp);        
        if(e==null) {
            HashIntIntMap newPreds=HashIntIntMaps.newUpdatableMap(n.preds);
            assert newPreds.containsKey(cp);
            newPreds.remove(cp);
            Node fromNode=this.fetchNode(newPreds);
            e=new Edge(fromNode, cp, p, n);
            linkEdge(e);
        }
        assert e.p==p;
        return e;
    }

    void linkEdge(Edge e) {
        
        assert !e.to.from.containsKey(e.cp);
        e.to.from.put(e.cp, e);
        
        Edge[] toEdges=this.fetchToEdges(e.from, e.cp);

        assert toEdges[e.p]==null;
        toEdges[e.p]=e;
    }





    public void supersets(Node n,Predicate<Node> f) {
        
        Queue<Node> q=new LinkedList<>();
        Object t=Object.class;
        IdentityHashMap<Node,Object> visited=new IdentityHashMap<>();
        q.add(n);
        visited.put(n,t);

        while (!q.isEmpty()) {

            n=q.poll();

            boolean stop=f.test(n);
            if(stop) continue;

            for(IntObjCursor<Edge[]> cur= n.to.cursor();cur.moveNext();) {
                Edge[] cpEdges=cur.value();
                for(int p=0;p<cpEdges.length;p++){
                    Edge nextEdge=cpEdges[p];
                    if(nextEdge==null)continue;
                    Node next=nextEdge.to;

                    if(visited.put(next,t)==null)
                    {
                        q.add(next);
                    } 
                }          
            }  
        }      
    }


    public void subsets(Node n,Predicate<Node> f) {
        
        Queue<Node> q=new LinkedList<>();
        Object t=Object.class;
        IdentityHashMap<Node,Object> visited=new IdentityHashMap<>();
        q.add(n);
        visited.put(n,t);

        while (!q.isEmpty()) {

            n=q.poll();

            boolean stop=f.test(n);
            if(stop) continue;

            for(IntObjCursor<Edge> cur= n.from.cursor();cur.moveNext();) {
                Edge nextEdge=cur.value();
                Node next=nextEdge.from;

                if(visited.put(next,t)==null)
                {
                    q.add(next);
                } 
            }          
        }      
    }
    
    
}
