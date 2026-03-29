package org.apache.cassandra.config;

import org.apache.cassandra.utils.FBUtilities;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

public class AccessControlConfig {
    public String allow_list;
    public String deny_list;
    
    public boolean use_allow_list;
    public boolean use_deny_list;
    public int range_query_mode;
    
    public int gossip_interval_ms = 1000;
 
    private transient List<InetAddress> allow_list_cached;
    private transient List<InetAddress> deny_list_cached;
    
    // Method to convert allow_list strings into a List of InetAddress
    public synchronized List<InetAddress> getAllowListAsInetAddresses() {
        if(allow_list_cached == null) { 
            allow_list_cached = convertToInetAddresses(allow_list);

            System.out.println("Allow List Initialized:");
            for(InetAddress allow : allow_list_cached) {
                System.out.println(" - " + allow.toString());
            }
        }
        return allow_list_cached;
    }

    // Method to convert deny_list strings into a List of InetAddress
    public synchronized List<InetAddress> getDenyListAsInetAddresses() {
        if(deny_list_cached == null) {
            deny_list_cached = convertToInetAddresses(deny_list);

            System.out.println("Deny List Initialized:");
            for(InetAddress deny : deny_list_cached) {
                System.out.println(" - " + deny.toString());
            }
        }
        return deny_list_cached;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccessControlConfig that = (AccessControlConfig) o;
        return use_allow_list == that.use_allow_list &&
                use_deny_list == that.use_deny_list &&
        range_query_mode == that.range_query_mode &&
                Objects.equals(allow_list, that.allow_list) && 
                Objects.equals(deny_list, that.deny_list);
    }
 
    @Override
    public int hashCode() {
        return Objects.hash(allow_list, deny_list, use_allow_list, use_deny_list, range_query_mode);
    }
    
    private transient Collection<InetAddress> allLocalAddresses = null;

    public synchronized ERangeSliceQueryMode getSelectedRangeSliceQueryMode() {
        ERangeSliceQueryMode[] values = ERangeSliceQueryMode.values();
        if (range_query_mode >= 0 && range_query_mode < values.length) {
            return values[range_query_mode];
        }
        
        System.err.println("An invalid value was provided for range_query_mode, falling back to default (None).");
        return ERangeSliceQueryMode.None;
    }
     
    public boolean isAddressAccessible(InetAddress address) {
        return isAddressAllowed(address) && !isAddressDenied(address);
    }
    
    private synchronized boolean isAddressAllowed(InetAddress address) {
        if(!use_allow_list) return true;

        try { 
            if (allLocalAddresses == null) {
                allLocalAddresses = FBUtilities.getAllLocalAddresses();
            }
            for (InetAddress allowedAddress : allLocalAddresses) {
                if (address.equals(allowedAddress)) {
                    return true;
                }
            }
        } catch (Exception e) { 
            System.err.println("AccessControlConfig isAddressAllowed threw an exception: " + e.getMessage());
        }
        
        for (InetAddress allowedAddress : getAllowListAsInetAddresses()) {
            if (address.equals(allowedAddress)) {
                return true;
            }
        }
        return false;
    }

    private synchronized boolean isAddressDenied(InetAddress address) {
        if(!use_deny_list) return false;
        for (InetAddress denyAddress : getDenyListAsInetAddresses()) {
            if (address.equals(denyAddress)) {
                return true;
            }
        }
        return false;
    }

    public List<InetAddress> convertToInetAddresses(String csv) {
        if (csv == null || csv.isEmpty()) {
            return Collections.emptyList(); // Return an empty list if input is null or empty
        }

        return Arrays.stream(csv.split(","))
                .map(this::safeGetByName)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private InetAddress safeGetByName(String address) {
        try {
            return InetAddress.getByName(address);
        } catch (UnknownHostException e) {
            System.out.println("Failed to resolve host: " + address + ", " + e.getMessage());
            return null; // Return null if host cannot be resolved, to be filtered out later
        }
    }
    // Add getters and setters if necessary
    public static ConfigLoader<AccessControlConfig> Loader = new ConfigLoader<>("access_control.yaml", AccessControlConfig.class, AccessControlConfig::new);
}
