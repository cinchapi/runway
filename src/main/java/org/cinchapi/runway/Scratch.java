package org.cinchapi.runway;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

public class Scratch {
    
    
    public static void main(String...args){
        
        RangeMap<Integer, String> map = TreeRangeMap.create();
        map.put(Range.open(1, 10), "X");
        map.put(Range.open(3, 12), "Y");
        map.remove(Range.closed(7, 9));
        System.out.println(map);
        
    }
    
    

}
