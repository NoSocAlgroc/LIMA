package edu.upc;

import org.junit.Test;

import com.koloboke.collect.map.hash.HashIntIntMaps;

import edu.upc.data.lattice.Lattice;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.function.Predicate;
public class TestLattice {

    @Test
    public void testLattice() {


        int[] preds=new int[]{1,3,1,8,2,1,1};
        Lattice<Object,Object> lattice=new Lattice<>(preds);

        Lattice<Object,Object>.Node root=lattice.fetchNode(HashIntIntMaps.newMutableMap());

        lattice.supersets(root,new Predicate<Lattice<Object,Object>.Node>(){
            @Override
            public boolean test(Lattice<Object,Object>.Node t) {

                System.out.println(t);
                for(int cp=0;cp<preds.length;cp++) {
                    if(t.preds.containsKey(cp))continue;
                    for(int p=0;p<preds[cp];p++) {
                        lattice.fetchTo(t, cp, p);
                    }
                }
                return false;
            }
            
        });

        int prod=1;
        for(int p:preds)prod*=(p+1);
        assertEquals(lattice.nodes.size(),prod);


        int count[]=new int[]{0};

        lattice.supersets(root, new Predicate<Lattice<Object,Object>.Node>() {
            @Override
            public boolean test(Lattice<Object,Object>.Node t) {
                count[0]+=1;
                System.out.println(t);
                return false;
            }
        });

        assertEquals(count[0], prod);





        lattice.supersets(root, new Predicate<Lattice<Object,Object>.Node>() {
            @Override
            public boolean test(Lattice<Object,Object>.Node t) {
                int npreds=t.preds.size();

                assertEquals(t.from.size(), npreds);
                assertEquals(t.to.size(), preds.length-npreds);
                
                for(int cp=0;cp<preds.length;cp++)
                {
                    if(t.preds.containsKey(cp)){
                        assertNull(t.to.get(cp));                        
                        assertNotNull(t.from.get(cp));
                    }
                    else{
                        assertNull(t.from.get(cp));
                        Lattice<Object,Object>.Edge[] cpPreds=t.to.get(cp);
                        for(Lattice<Object,Object>.Edge e:cpPreds) 
                        {
                            assertNotNull(e);
                        }
                    }                    
                }

                
                //System.out.println(t);
                return false;
            }
        });



    }


    @Test
    public void testSubLattice() 
    {
        int[] preds=new int[]{1,1,1};

        Lattice<Integer,Integer> base=new Lattice<>(preds);

        Lattice<String,Object> a=new Lattice<>(base),b=new Lattice<>(base);
        Lattice<String,Object>.Node an=a.fetchRoot(),bn=b.fetchRoot();

        assertEquals(an.base, bn.base);

        int cp=0,p=0;
        Lattice<String,Object>.Edge ae=a.fetchTo(an, cp, p),be=b.fetchTo(bn, cp, p);

        assertEquals(ae.base, be.base);

        an=ae.to;
        bn=be.to;


        assertEquals(an.base, bn.base);

        cp=1;
        p=0;
        ae=a.fetchTo(an, cp, p);
        be=b.fetchTo(bn, cp, p);

        assertEquals(ae.base, be.base);

        an=ae.to;
        bn=be.to;

        assertEquals(an.base, bn.base);

        cp=2;
        p=0;
        ae=a.fetchTo(an, cp, p);
        be=b.fetchTo(bn, cp, p);

        assertEquals(ae.base, be.base);

        an=ae.to;
        bn=be.to;

        assertEquals(ae.base, be.base);
    }
    
    
}
