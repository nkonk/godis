package main

import ("fmt"
        "net/http"
        "sync"
        "net/url"
)

var kvsLock sync.RWMutex
var kvsStore map[string]string



func main(){
    kvsStore = make(map[string]string)
    kvsLock = sync.RWMutex{}
    
    http.HandleFunc("/get",get)
    http.HandleFunc("/set",set)
    http.HandleFunc("/list",list)
    http.HandleFunc("/remove",remove)
    http.ListenAndServe(":3000",nil)   
 
}

func get(w http.ResponseWriter,r *http.Request){
    if r.Method == http.MethodGet {
    values,err := url.ParseQuery(r.URL.RawQuery)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w,"Error: ",err)
            return
        }
        if len(values.Get("key")) == 0 {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w,"Error: Invalid Key")
            return
        }
        
        //Things are valid , get key from store
        
        //grab mutex
        kvsLock.RLock()
        value := kvsStore[string(values.Get("key"))]
        kvsLock.RUnlock()
        
        //Print it out for now
        
        fmt.Fprint(w,value)
        
    }else{
         w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w,"Error: Only GET request is supported.")
            return
    }
}

func set(w http.ResponseWriter,r *http.Request){
        if(r.Method == http.MethodPost) {
        values, err := url.ParseQuery(r.URL.RawQuery)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Error:", err)
            return
        }
        if len(values.Get("key")) == 0 {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Error:", "Wrong input key.")
            return
        }
        if len(values.Get("value")) == 0 {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Error:", "Wrong input value.")
            return
        }
        
        kvsLock.Lock()
        
        kvsStore[values.Get("key")] = string(values.Get("value"))
        //Length is not controlled here. Open for some fun :P
        
        kvsLock.UnLock()
        
        fmt.Fprint(w,"Success")
        } else {
            http.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w,"Error: Only POST accepted")
        }
}



func list(w http.ResponseWriter,r *http.Request){
   if r.Method == http.MethodGet {
       kvsLock.RLock()
       for key,val := range kvsStore {
           fmt.Fprint(w,key,":",val)
           //FixMe: Try storing the keys and values in a variable before writing it out to net. this will reduce the mutex lockin period.
       }
       kvsLock.RUnlock()
       
   } else {
          http.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w,"Error: Only GET accepted")
   } 
}

func remove(w http.ResponseWriter, r *http.Request) {
    if(r.Method == http.MethodDelete) {
        values, err := url.ParseQuery(r.URL.RawQuery)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Error:", err)
            return
        }
        if len(values.Get("key")) == 0 {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Error:", "Wrong input key.")
            return
        }

        kVStoreMutex.Lock()
        delete(keyValueStore, values.Get("key"))
        kVStoreMutex.Unlock()

        fmt.Fprint(w, "success")
    } else {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprint(w, "Error: Only DELETE accepted.")
    }
} 
