package org.apache.cassandra.config;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.representer.Representer;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.*;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ConfigLoader<TConfig> {

    private final String _configFileName; 
    private static final String CONFIG_DIRECTORY_PATH = "C:\\var\\lib\\cassandra";
    private TConfig currentConfig;    
    private TConfig defaultConfig;
    private final PropertyChangeSupport pcs = new PropertyChangeSupport(this);
    private boolean _hasLoaded = false;
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private long lastModifiedTime = 0;
    private final Class<TConfig> configClass;
    
    public ConfigLoader(String configFileName, Class<TConfig> configClass, Supplier<TConfig> configSupplier) {
        this._configFileName = configFileName;
        this.configClass = configClass;
        this.currentConfig = configSupplier.get();
        this.defaultConfig = this.currentConfig;
        loadConfig();
        startTimerTask();
    }
    private void startTimerTask() {
        executorService.scheduleAtFixedRate(() -> {
            try {
                checkAndReloadConfig();
            }
            catch(Exception e) {
                System.out.println("Problem checking Configuration file " + e.getMessage());
            }
        }, 0, 5, TimeUnit.SECONDS);
    }
    
    private void checkAndReloadConfig() {
        File configFile = new File(CONFIG_DIRECTORY_PATH, _configFileName);
        if(!configFile.exists()) {
            currentConfig = defaultConfig;
        }
        
        long currentModifiedTime = configFile.lastModified();
        if(currentModifiedTime > lastModifiedTime) {
            loadConfig();
            lastModifiedTime = currentModifiedTime;
        }
    }
    
    public synchronized void loadConfig() {
        _hasLoaded = true;
        try(InputStream inputStream = new FileInputStream(Paths.get(CONFIG_DIRECTORY_PATH, _configFileName).toString())) {
            Representer representer = new Representer();
            representer.getPropertyUtils().setSkipMissingProperties(true);
            Yaml yaml = new Yaml(new Constructor(configClass), representer);
            TConfig newConfig = yaml.loadAs(inputStream, configClass);
            TConfig oldConfig = currentConfig;

            if (Objects.equals(newConfig, oldConfig)) return;
            currentConfig = newConfig;
            System.out.println("Configuration loaded/refreshed.");
            pcs.firePropertyChange("config", oldConfig, newConfig);
        } 
        catch(FileNotFoundException e) { 
            System.out.println("Problem loading configuration: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("Problem loading configuration: " + e.getMessage());
        } catch(Exception e) { 
            System.out.println("Problem loading configuration: " + e.getClass().getName() + " " + e.getMessage());
        }
    }
 
    public void addPropertyChangeListener(PropertyChangeListener listener) {
        pcs.addPropertyChangeListener(listener);
    }

    public void removePropertyChangeListener(PropertyChangeListener listener) {
        pcs.removePropertyChangeListener(listener);
    }

    public synchronized TConfig getCurrentConfig() {
        if (!_hasLoaded) {
            loadConfig();
        }
        return currentConfig;
    }

}

