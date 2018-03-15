package sparktemplate.association;

import sparktemplate.ASettings;

import java.util.HashMap;

/**
 * Created by as on 15.03.2018.
 */
public class AssociationSettings implements ASettings<AssociationSettings> {

    HashMap<String,String> hashMap;

    public AssociationSettings() {
        this.hashMap = new HashMap();
    }

    @Override
    public boolean hasKey(String key) {
        return hashMap.containsKey(key);
    }

    @Override
    public String getValue(String key) {
        return hashMap.get(key);
    }

    @Override
    public AssociationSettings setting(String key, String value) {
        this.hashMap.put(key,value);
        return this;
    }
}
