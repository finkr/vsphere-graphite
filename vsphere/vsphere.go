package vsphere

import (
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cblomart/vsphere-graphite/backend"
	"github.com/cblomart/vsphere-graphite/utils"

	"golang.org/x/net/context"

	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/vim25/methods"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
)

var stdlog, errlog *log.Logger

// VCenter description
type VCenter struct {
	Hostname     string
	Username     string
	Password     string
	MetricGroups []*MetricGroup
}

// MetricGroup Grouping for retrieval
type MetricGroup struct {
	ObjectType string
	Metrics    []*MetricDef
	Mor        []*types.ManagedObjectReference
}

// MetricDef Definition
type MetricDef struct {
	Metric    string
	Instances string
	Key       int32
}

// Metric description in config
type Metric struct {
	ObjectType []string
	Definition []*MetricDef
}

// Properties describes know relation to properties to related objects and properties
var Properties = map[string]map[string][]string{
	"datastore": map[string][]string{
		"Datastore":      []string{"name"},
		"VirtualMachine": []string{"datastore"},
	},
	"host": map[string][]string{
		"HostSystem":     []string{"name", "parent"},
		"VirtualMachine": []string{"name", "runtime.host"},
	},
	"cluster": map[string][]string{
		"ClusterComputeResource": []string{"name"},
	},
	"network": map[string][]string{
		"DistributedVirtualPortgroup": []string{"name"},
		"Network":                     []string{"name"},
		"VirtualMachine":              []string{"network"},
	},
	"resourcepool": map[string][]string{
		"ResourcePool": []string{"name", "parent", "vm"},
	},
	"folder": map[string][]string{
		"Folder":         []string{"name", "parent"},
		"VirtualMachine": []string{"parent"},
	},
	"tags": map[string][]string{
		"VirtualMachine": []string{"tag"},
		"HostSystem":     []string{"tag"},
	},
	"numcpu": map[string][]string{
		"VirtualMachine": []string{"summary.config.numCpu"},
	},
	"memorysizemb": map[string][]string{
		"VirtualMachine": []string{"summary.config.memorySizeMB"},
	},
	"disks": map[string][]string{
		"VirtualMachine": []string{"guest.disk"},
	},
}

// AddMetric : add a metric definition to a metric group
func (vcenter *VCenter) AddMetric(metric *MetricDef, mtype string) {
	// find the metric group for the type
	var metricGroup *MetricGroup
	for _, tmp := range vcenter.MetricGroups {
		if tmp.ObjectType == mtype {
			metricGroup = tmp
			break
		}
	}
	// create a new group if needed
	if metricGroup == nil {
		metricGroup = &MetricGroup{ObjectType: mtype}
		vcenter.MetricGroups = append(vcenter.MetricGroups, metricGroup)
	}
	// check if Metric already present
	for _, tmp := range metricGroup.Metrics {
		if tmp.Key == metric.Key {
			// metric already in metric group
			return
		}
	}
	metricGroup.Metrics = append(metricGroup.Metrics, metric)
}

// Connect : Conncet to vcenter
func (vcenter *VCenter) Connect() (*govmomi.Client, error) {
	// Prepare vCenter Connections
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stdlog.Println("connecting to vcenter: " + vcenter.Hostname)
	username := url.QueryEscape(vcenter.Username)
	password := url.QueryEscape(vcenter.Password)
	u, err := url.Parse("https://" + username + ":" + password + "@" + vcenter.Hostname + "/sdk")
	if err != nil {
		errlog.Println("Could not parse vcenter url: ", vcenter.Hostname)
		errlog.Println("Error: ", err)
		return nil, err
	}
	client, err := govmomi.NewClient(ctx, u, true)
	if err != nil {
		errlog.Println("Could not connect to vcenter: ", vcenter.Hostname)
		errlog.Println("Error: ", err)
		return nil, err
	}
	return client, nil
}

// InitMetrics : maps metric keys to requested metrics
func InitMetrics(metrics []*Metric, perfmanager *mo.PerformanceManager) {
	// build a map of perf key per metric string id
	metricToPerf := make(map[string]int32)
	for _, perf := range perfmanager.PerfCounter {
		key := fmt.Sprintf("%s.%s.%s", perf.GroupInfo.GetElementDescription().Key, perf.NameInfo.GetElementDescription().Key, string(perf.RollupType))
		metricToPerf[key] = perf.Key
	}
	// fill in the metric key
	for _, metric := range metrics {
		for _, metricdef := range metric.Definition {
			if key, ok := metricToPerf[metricdef.Metric]; ok {
				metricdef.Key = key
			}
		}
	}
}

// Init : initialize vcenter
func (vcenter *VCenter) Init(metrics []*Metric, standardLogs *log.Logger, errorLogs *log.Logger) {
	stdlog = standardLogs
	errlog = errorLogs
	// connect to vcenter
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := vcenter.Connect()
	if err != nil {
		errlog.Println("Could not connect to vcenter: ", vcenter.Hostname)
		errlog.Println("Error: ", err)
		return
	}
	defer func() {
		err := client.Logout(ctx) // nolint: vetshadow
		if err != nil {
			errlog.Println("Error loging out of vcenter: ", vcenter.Hostname)
			errlog.Println("Error: ", err)
		}
	}()
	// get the performance manager
	var perfmanager mo.PerformanceManager
	err = client.RetrieveOne(ctx, *client.ServiceContent.PerfManager, nil, &perfmanager)
	if err != nil {
		errlog.Println("Could not get performance manager")
		errlog.Println("Error: ", err)
		return
	}
	InitMetrics(metrics, &perfmanager)
	// add the metric to be collected in vcenter
	for _, metric := range metrics {
		for _, metricdef := range metric.Definition {
			if metricdef.Key == 0 {
				stdlog.Println("Metric key was not found for " + metricdef.Metric)
				continue
			}
			for _, mtype := range metric.ObjectType {
				vcenter.AddMetric(metricdef, mtype)
			}
		}
	}
}

// Query : Query a vcenter
func (vcenter *VCenter) Query(interval int, domain string, properties []string, channel *chan backend.Point) {
	stdlog.Println("Setting up query inventory of vcenter: ", vcenter.Hostname)

	// Create the contect
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Get the client
	client, err := vcenter.Connect()
	if err != nil {
		errlog.Println("Could not connect to vcenter: ", vcenter.Hostname)
		errlog.Println("Error: ", err)
		return
	}

	// wait to be properly connected to defer logout
	defer func() {
		stdlog.Println("disconnecting from vcenter:", vcenter.Hostname)
		err := client.Logout(ctx) // nolint: vetshadow
		if err != nil {
			errlog.Println("Error loging out of vcenter: ", vcenter.Hostname)
			errlog.Println("Error: ", err)
		}
	}()

	// Create the view manager
	var viewManager mo.ViewManager
	err = client.RetrieveOne(ctx, *client.ServiceContent.ViewManager, nil, &viewManager)
	if err != nil {
		errlog.Println("Could not get view manager from vcenter: " + vcenter.Hostname)
		errlog.Println("Error: ", err)
		return
	}

	// Find all Datacenters accessed by the user
	datacenters := []types.ManagedObjectReference{}
	finder := find.NewFinder(client.Client, true)
	dcs, err := finder.DatacenterList(ctx, "*")
	if err != nil {
		errlog.Println("Could not find a datacenter: ", vcenter.Hostname)
		errlog.Println("Error: ", err)
		return
	}
	for _, child := range dcs {
		datacenters = append(datacenters, child.Reference())
	}

	// Get interesting objects from properties
	// Check if contains all
	objectTypes := []string{}
	// replace all by all properties
	all := false
	for _, property := range properties {
		if strings.ToLower(property) == "all" {
			all = true
			break
		}
	}
	if all {
		properties = []string{}
		for propkey := range Properties {
			properties = append(properties, propkey)
		}
	}
	// get the object types from properties
	for _, property := range properties {
		if propval, ok := Properties[property]; ok {
			for objkey := range propval {
				objectTypes = append(objectTypes, objkey)
			}
		}
	}
	// Get interesting object types from specified queries
	//objectTypes := []string{"ClusterComputeResource", "Datastore", "HostSystem", "DistributedVirtualPortgroup", "Network", "ResourcePool", "Folder"}
	// Complete object of interest with required metrics
	for _, group := range vcenter.MetricGroups {
		found := false
		for _, tmp := range objectTypes {
			if group.ObjectType == tmp {
				found = true
				break
			}
		}
		if !found {
			objectTypes = append(objectTypes, group.ObjectType)
		}
	}

	// Loop trought datacenters and create the intersting object reference list
	mors := []types.ManagedObjectReference{}
	for _, datacenter := range datacenters {
		// Create the CreateContentView request
		req := types.CreateContainerView{This: viewManager.Reference(), Container: datacenter, Type: objectTypes, Recursive: true}
		res, err := methods.CreateContainerView(ctx, client.RoundTripper, &req) // nolint: vetshadow
		if err != nil {
			errlog.Println("Could not create container view from vcenter: " + vcenter.Hostname)
			errlog.Println("Error: ", err)
			continue
		}
		// Retrieve the created ContentView
		var containerView mo.ContainerView
		err = client.RetrieveOne(ctx, res.Returnval, nil, &containerView)
		if err != nil {
			errlog.Println("Could not get container view from vcenter: " + vcenter.Hostname)
			errlog.Println("Error: ", err)
			continue
		}
		// Add found object to object list
		mors = append(mors, containerView.View...)
	}

	if len(mors) == 0 {
		errlog.Printf("No object of intereset in types %s\n", strings.Join(objectTypes, ", "))
		return
	}

	//object for propery collection
	var objectSet []types.ObjectSpec
	for _, mor := range mors {
		objectSet = append(objectSet, types.ObjectSpec{Obj: mor, Skip: types.NewBool(false)})
	}

	//properties specifications
	reqProps := make(map[string][]string)
	//first fill in name for each object type
	for _, objType := range objectTypes {
		reqProps[objType] = []string{"name"}
	}
	//complete with required properties
	for _, property := range properties {
		if typeProperties, ok := Properties[property]; ok {
			for objType, typeProperties := range typeProperties {
				for _, typeProperty := range typeProperties {
					found := false
					for _, prop := range reqProps[objType] {
						if prop == typeProperty {
							found = true
							break
						}
					}
					if !found {
						reqProperties := append(reqProps[objType], typeProperty)
						reqProps[objType] = reqProperties
					}
				}
			}
		}
	}
	propSet := []types.PropertySpec{}
	for objType, props := range reqProps {
		propSet = append(propSet, types.PropertySpec{Type: objType, PathSet: props})
	}
	//propSet = append(propSet, types.PropertySpec{Type: "ManagedEntity", PathSet: []string{"name", "parent", "tag"}})
	//propSet = append(propSet, types.PropertySpec{Type: "VirtualMachine", PathSet: []string{"datastore", "network", "runtime.host", "summary.config.numCpu", "summary.config.memorySizeMB", "guest.disk"}})
	//propSet = append(propSet, types.PropertySpec{Type: "ResourcePool", PathSet: []string{"vm"}})

	//retrieve properties
	propreq := types.RetrieveProperties{SpecSet: []types.PropertyFilterSpec{{ObjectSet: objectSet, PropSet: propSet}}}
	propres, err := client.PropertyCollector().RetrieveProperties(ctx, propreq)
	if err != nil {
		errlog.Println("Could not retrieve object names from vcenter: " + vcenter.Hostname)
		errlog.Println("Error: ", err)
		return
	}

	//create a map to resolve object names
	morToName := make(map[types.ManagedObjectReference]string)

	//create a map to resolve vm to datastore
	vmToDatastore := make(map[types.ManagedObjectReference][]types.ManagedObjectReference)

	//create a map to resolve vm to network
	vmToNetwork := make(map[types.ManagedObjectReference][]types.ManagedObjectReference)

	//create a map to resolve vm to host
	vmToHost := make(map[types.ManagedObjectReference]types.ManagedObjectReference)

	//create a map to resolve host to parent - in a cluster the parent should be a cluster
	morToParent := make(map[types.ManagedObjectReference]types.ManagedObjectReference)

	//create a map to resolve mor to vms - object that contains multiple vms as child objects
	morToVms := make(map[types.ManagedObjectReference][]types.ManagedObjectReference)

	//create a map to resolve mor to tags
	morToTags := make(map[types.ManagedObjectReference][]types.Tag)

	//create a map to resolve mor to numcpu
	morToNumCPU := make(map[types.ManagedObjectReference]int32)

	//create a map to resolve mor to memorysizemb
	morToMemorySizeMB := make(map[types.ManagedObjectReference]int32)

	//create a map to resolve diskinfos
	morToDiskInfos := make(map[types.ManagedObjectReference][]types.GuestDiskInfo)

	for _, objectContent := range propres.Returnval {
		for _, Property := range objectContent.PropSet {
			switch propertyName := Property.Name; propertyName {
			case "name":
				name, ok := Property.Val.(string)
				if ok {
					morToName[objectContent.Obj] = name
				} else {
					errlog.Println("Name property of " + objectContent.Obj.String() + " was not a string, it was " + fmt.Sprintf("%T", Property.Val))
				}
			case "datastore":
				err := utils.MapObjRefs(propertyName, Property.Val, vmToDatastore, objectContent.Obj)
				if err != nil {
					errlog.Println(err)
				}
			case "network":
				err := utils.MapObjRefs(propertyName, Property.Val, vmToNetwork, objectContent.Obj)
				if err != nil {
					errlog.Println(err)
				}
			case "runtime.host":
				err := utils.MapObjRef(propertyName, Property.Val, vmToHost, objectContent.Obj)
				if err != nil {
					errlog.Println(err)
				}
			case "parent":
				err := utils.MapObjRef(propertyName, Property.Val, morToParent, objectContent.Obj)
				if err != nil {
					errlog.Println(err)
				}
			case "vm":
				err := utils.MapObjRefs(propertyName, Property.Val, morToVms, objectContent.Obj)
				if err != nil {
					errlog.Println(err)
				}
			case "tag":
				tags, ok := Property.Val.(types.ArrayOfTag)
				if ok {
					if len(tags.Tag) > 0 {
						morToTags[objectContent.Obj] = tags.Tag
					}
				} else {
					errlog.Println("Tag property of " + objectContent.Obj.String() + " was not an array of Tag, it was " + fmt.Sprintf("%T", Property.Val))
				}
			case "summary.config.numCpu":
				err := utils.MapObjInt32(propertyName, Property.Val, morToNumCPU, objectContent.Obj)
				if err != nil {
					errlog.Println(err)
				}
			case "summary.config.memorySizeMB":
				err := utils.MapObjInt32(propertyName, Property.Val, morToMemorySizeMB, objectContent.Obj)
				if err != nil {
					errlog.Println(err)
				}
			case "guest.disk":
				diskInfos, ok := Property.Val.(types.ArrayOfGuestDiskInfo)
				if ok {
					if len(diskInfos.GuestDiskInfo) > 0 {
						morToDiskInfos[objectContent.Obj] = diskInfos.GuestDiskInfo
					}
				}
			default:
				errlog.Println("Unhandled property '" + propertyName + "' for " + objectContent.Obj.String() + " whose type is " + fmt.Sprintf("%T", Property.Val))
			}
		}
	}

	//create a map to resolve metric names
	metricToName := make(map[int32]string)
	for _, metricgroup := range vcenter.MetricGroups {
		for _, metricdef := range metricgroup.Metrics {
			metricToName[metricdef.Key] = metricdef.Metric
		}
	}

	//create a map to resolve vm to their ressourcepool
	vmToResourcePoolPath := make(map[types.ManagedObjectReference]string)
	for mor, vmmors := range morToVms {
		// not doing case sensitive as this could be extensive
		if mor.Type == "ResourcePool" {
			// find the full path of the resource pool
			var poolpath = ""
			var poolmor = mor
			var ok = true
			for ok {
				poolname, ok := morToName[poolmor]
				if !ok {
					// could not find name
					errlog.Println("Could not find name for resourcepool " + mor.String())
					break
				}
				if poolname == "Resources" {
					// ignore root resourcepool
					break
				}
				// add the name to the path
				poolpath = poolname + "/" + poolpath
				newmor, ok := morToParent[poolmor]
				if !ok {
					// no parent pool found
					errlog.Println("Could not find parent for resourcepool " + poolname)
					break
				}
				poolmor = newmor
				if poolmor.Type != "ResourcePool" {
					break
				}
			}
			poolpath = strings.Trim(poolpath, "/")
			for _, vmmor := range vmmors {
				if vmmor.Type == "VirtualMachine" {
					vmToResourcePoolPath[vmmor] = poolpath
				}
			}
		}
	}

	// Create a map from folder to path
	folderMorToPath := make(map[types.ManagedObjectReference]string)

	// Create Queries from interesting objects and requested metrics

	queries := []types.PerfQuerySpec{}

	// Common parameters
	intervalID := int32(20)
	endTime := time.Now().Add(time.Duration(-1) * time.Second)
	startTime := endTime.Add(time.Duration(-interval-1) * time.Second)

	// Parse objects
	for _, mor := range mors {
		metricIds := []types.PerfMetricId{}
		for _, metricgroup := range vcenter.MetricGroups {
			if metricgroup.ObjectType == mor.Type {
				for _, metricdef := range metricgroup.Metrics {
					metricIds = append(metricIds, types.PerfMetricId{CounterId: metricdef.Key, Instance: metricdef.Instances})
				}
			}
		}
		if len(metricIds) > 0 {
			queries = append(queries, types.PerfQuerySpec{Entity: mor, StartTime: &startTime, EndTime: &endTime, MetricId: metricIds, IntervalId: intervalID})
		}
	}

	// Check that there is somethong to query
	querycount := len(queries)
	if querycount == 0 {
		errlog.Println("No queries created!")
		return
	}
	metriccount := 0
	for _, query := range queries {
		metriccount = metriccount + len(query.MetricId)
	}
	stdlog.Println("Issuing " + strconv.Itoa(querycount) + " queries to vcenter " + vcenter.Hostname + " requesting " + strconv.Itoa(metriccount) + " metrics.")

	// Query the performances
	perfreq := types.QueryPerf{This: *client.ServiceContent.PerfManager, QuerySpec: queries}
	perfres, err := methods.QueryPerf(ctx, client.RoundTripper, &perfreq)
	if err != nil {
		errlog.Println("Could not request perfs from vcenter: " + vcenter.Hostname)
		errlog.Println("Error: ", err)
		return
	}

	// Get the result
	vcName := strings.Replace(vcenter.Hostname, domain, "", -1)
	returncount := len(perfres.Returnval)
	if returncount == 0 {
		errlog.Println("No result returned by queries.")
	}
	valuescount := 0
	for _, base := range perfres.Returnval {
		pem := base.(*types.PerfEntityMetric)
		//entityName := strings.ToLower(pem.Entity.Type)
		name := strings.ToLower(strings.Replace(morToName[pem.Entity], domain, "", -1))
		//find datastore
		datastore := []string{}
		if mors, ok := vmToDatastore[pem.Entity]; ok {
			for _, mor := range mors {
				datastore = append(datastore, morToName[mor])
			}
		}
		//find host and cluster
		vmhost := ""
		cluster := ""
		if len(vmToHost) > 0 {
			vmhost, cluster, err = utils.FindHostAndCluster(pem.Entity, vmToHost, morToParent, morToName)
			if err != nil {
				errlog.Println(err)
			}
		}
		vmhost = strings.Replace(vmhost, domain, "", -1)
		//find network
		network := []string{}
		if len(vmToNetwork) > 0 {
			if mors, ok := vmToNetwork[pem.Entity]; ok {
				for _, mor := range mors {
					network = append(network, morToName[mor])
				}
			}
		}
		//find resource pool path
		resourcepool := ""
		if len(vmToResourcePoolPath) > 0 {
			if rppath, ok := vmToResourcePoolPath[pem.Entity]; ok {
				resourcepool = rppath
			}
		}
		//find folder path
		if len(morToParent) > 0 {
			if folders, ok := folderMorToPath[pem.Entity]; !ok {
				if pem.Entity.Type != "HostSystem" {
					current, ok := morToParent[pem.Entity]
					for ok {
						if current.Type != "Folder" {
							errlog.Println("Parent is not a folder for " + current.String())
							break
						}
						foldername, ok := morToName[current]
						if !ok {
							errlog.Println("Folder name not found for " + current.String())
							break
						}
						if foldername == "vm" {
							break
						}
						folders = foldername + "/" + folders
						newcurrent, ok := morToParent[current]
						if !ok {
							errlog.Println("No parent found for folder " + current.String())
							break
						}
						current = newcurrent
					}
					folders = strings.Trim(folders, "/")
				}
				folderMorToPath[pem.Entity] = folders
			}
		}
		//find tags
		vitags := []string{}
		if tags, ok := morToTags[pem.Entity]; ok {
			for _, tag := range tags {
				vitags = append(vitags, tag.Key)
			}
		}
		//find numcpu
		var numcpu int32
		if len(morToNumCPU) > 0 {
			numcpu = morToNumCPU[pem.Entity]
		}
		//find memorysizemb
		var memorysizemb int32
		if len(morToMemorySizeMB) > 0 {
			memorysizemb = morToMemorySizeMB[pem.Entity]
		}
		if len(pem.Value) == 0 {
			errlog.Println("No values returned in query!")
		}
		//find folder
		folder := folderMorToPath[pem.Entity]
		folder = strings.Replace(folder, " ", "\\ ", -1)
		folder = strings.Replace(folder, ",", "\\,", -1)
		objType := strings.ToLower(pem.Entity.Type)
		timeStamp := endTime.Unix()
		//send disk infos
		if diskInfos, ok := morToDiskInfos[pem.Entity]; ok {
			for _, diskInfo := range diskInfos {
				// skip if no capacity
				if diskInfo.Capacity == 0 {
					continue
				}
				// format disk path
				diskPath := strings.Replace(diskInfo.DiskPath, "\\", "/", -1)
				// send free space
				diskfree := backend.Point{
					VCenter:      vcName,
					ObjectType:   objType,
					ObjectName:   name,
					Group:        "guestdisk",
					Counter:      "freespace",
					Instance:     diskPath,
					Rollup:       "latest",
					Value:        diskInfo.FreeSpace,
					Datastore:    datastore,
					ESXi:         vmhost,
					Cluster:      cluster,
					Network:      network,
					ResourcePool: resourcepool,
					Folder:       folder,
					ViTags:       vitags,
					NumCPU:       numcpu,
					MemorySizeMB: memorysizemb,
					Timestamp:    timeStamp,
				}
				*channel <- diskfree
				// send capacity
				diskcapa := backend.Point{
					VCenter:      vcName,
					ObjectType:   objType,
					ObjectName:   name,
					Group:        "guestdisk",
					Counter:      "capacity",
					Instance:     diskPath,
					Rollup:       "latest",
					Value:        diskInfo.Capacity,
					Datastore:    datastore,
					ESXi:         vmhost,
					Cluster:      cluster,
					Network:      network,
					ResourcePool: resourcepool,
					Folder:       folder,
					ViTags:       vitags,
					NumCPU:       numcpu,
					MemorySizeMB: memorysizemb,
					Timestamp:    timeStamp,
				}
				*channel <- diskcapa
				// send usage %
				diskpc := backend.Point{
					VCenter:      vcName,
					ObjectType:   objType,
					ObjectName:   name,
					Group:        "guestdisk",
					Counter:      "usage",
					Instance:     diskPath,
					Rollup:       "latest",
					Value:        int64(10000 * (1 - (float64(diskInfo.FreeSpace) / float64(diskInfo.Capacity)))),
					Datastore:    datastore,
					ESXi:         vmhost,
					Cluster:      cluster,
					Network:      network,
					ResourcePool: resourcepool,
					ViTags:       vitags,
					NumCPU:       numcpu,
					MemorySizeMB: memorysizemb,
					Timestamp:    timeStamp,
				}
				*channel <- diskpc
			}
		}
		valuescount = valuescount + len(pem.Value)
		for _, baseserie := range pem.Value {
			serie := baseserie.(*types.PerfMetricIntSeries)
			metricName := strings.ToLower(metricToName[serie.Id.CounterId])
			instanceName := serie.Id.Instance
			var value int64 = -1
			switch {
			case strings.HasSuffix(metricName, ".average"):
				value = utils.Average(serie.Value...)
			case strings.HasSuffix(metricName, ".maximum"):
				value = utils.Max(serie.Value...)
			case strings.HasSuffix(metricName, ".minimum"):
				value = utils.Min(serie.Value...)
			case strings.HasSuffix(metricName, ".latest"):
				value = serie.Value[len(serie.Value)-1]
			case strings.HasSuffix(metricName, ".summation"):
				value = utils.Sum(serie.Value...)
			}
			metricparts := strings.Split(metricName, ".")
			point := backend.Point{
				VCenter:      vcName,
				ObjectType:   objType,
				ObjectName:   name,
				Group:        metricparts[0],
				Counter:      metricparts[1],
				Instance:     instanceName,
				Rollup:       metricparts[2],
				Value:        value,
				Datastore:    datastore,
				ESXi:         vmhost,
				Cluster:      cluster,
				Network:      network,
				ResourcePool: resourcepool,
				Folder:       folder,
				ViTags:       vitags,
				NumCPU:       numcpu,
				MemorySizeMB: memorysizemb,
				Timestamp:    timeStamp,
			}
			*channel <- point
		}
	}
	stdlog.Println("Got " + strconv.Itoa(returncount) + " results from " + vcenter.Hostname + " with " + strconv.Itoa(valuescount) + " values")
}
