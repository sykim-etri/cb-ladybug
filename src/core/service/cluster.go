package service

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cloud-barista/cb-ladybug/src/core/model"
	"github.com/cloud-barista/cb-ladybug/src/core/model/tumblebug"
	"github.com/cloud-barista/cb-ladybug/src/utils/config"
	"github.com/cloud-barista/cb-ladybug/src/utils/lang"
	ssh "github.com/cloud-barista/cb-spider/cloud-control-manager/vm-ssh"

	logger "github.com/sirupsen/logrus"
)

func ListCluster(namespace string) (*model.ClusterList, error) {
	clusters := model.NewClusterList(namespace)

	clusterKey := lang.GetStoreMCKSClusterKey(namespace, "")
	err := clusters.SelectList(clusterKey)
	if err != nil {
		return nil, err
	}

	return clusters, nil
}

func GetCluster(namespace string, clusterName string) (*model.Cluster, error) {
	cluster := model.NewCluster(namespace, clusterName)
	clusterKey := lang.GetStoreMCKSClusterKey(namespace, clusterName)
	err := cluster.Select(clusterKey)
	if err != nil {
		return nil, err
	}

	return cluster, nil
}

func createMCIS(cluster *model.Cluster, cpNcs *[]model.NodeConfig, wNcs *[]model.NodeConfig) (*tumblebug.MCIS, error) {
	// Namespace 존재여부 확인
	ns := tumblebug.NewNS(cluster.Namespace)
	exists, err := ns.GET()
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New(fmt.Sprintf("namespace does not exist (name=%s)", cluster.Namespace))
	}

	// MCIS 존재여부 확인
	mcis := tumblebug.NewMCIS(cluster.Namespace, cluster.Name)
	exists, err = mcis.GET()
	if err != nil {
		return nil, err
	}
	if exists {
		return mcis, errors.New("MCIS already exists")
	}

	var nodeConfigInfos []NodeConfigInfo
	// control plane
	cp, err := SetNodeConfigInfos(*cpNcs, config.CONTROL_PLANE)
	if err != nil {
		return nil, err
	}
	nodeConfigInfos = append(nodeConfigInfos, cp...)

	// worker
	wk, err := SetNodeConfigInfos(*wNcs, config.WORKER)
	if err != nil {
		return nil, err
	}
	nodeConfigInfos = append(nodeConfigInfos, wk...)

	cIdx := 0
	wIdx := 0
	var vmInfos []model.VMInfo

	for _, nodeConfigInfo := range nodeConfigInfos {
		// MCIR - 존재하면 재활용 없다면 생성 기준
		// 1. create vpc
		vpc, err := nodeConfigInfo.CreateVPC(cluster.Namespace)
		if err != nil {
			return nil, err
		}

		// 2. create firewall
		fw, err := nodeConfigInfo.CreateFirewall(cluster.Namespace)
		if err != nil {
			return nil, err
		}

		// 3. create sshKey
		sshKey, err := nodeConfigInfo.CreateSshKey(cluster.Namespace)
		if err != nil {
			return nil, err
		}

		// 4. create image
		image, err := nodeConfigInfo.CreateImage(cluster.Namespace)
		if err != nil {
			return nil, err
		}

		// 5. create spec
		spec, err := nodeConfigInfo.CreateSpec(cluster.Namespace)
		if err != nil {
			return nil, err
		}

		// 6. vm
		for i := 0; i < nodeConfigInfo.Count; i++ {
			if nodeConfigInfo.Role == config.CONTROL_PLANE {
				cIdx++
			} else {
				wIdx++
			}

			vm := model.VM{
				Config:       nodeConfigInfo.Connection,
				VPC:          vpc.Name,
				Subnet:       vpc.Subnets[0].Name,
				Firewall:     []string{fw.Name},
				SSHKey:       sshKey.Name,
				Image:        image.Name,
				Spec:         spec.Name,
				UserAccount:  nodeConfigInfo.Account,
				UserPassword: "",
				Description:  "",
			}

			vmInfo := model.VMInfo{
				Credential: sshKey.PrivateKey,
				Role:       nodeConfigInfo.Role,
				Csp:        nodeConfigInfo.Csp,
			}

			if nodeConfigInfo.Role == config.CONTROL_PLANE {
				vm.Name = lang.GetNodeName(cluster.Name, config.CONTROL_PLANE, cIdx)
				if cIdx == 1 {
					vmInfo.IsCPLeader = true
					cluster.CpLeader = vm.Name
				}
			} else {
				vm.Name = lang.GetNodeName(cluster.Name, config.WORKER, wIdx)
			}
			vmInfo.Name = vm.Name

			mcis.VMs = append(mcis.VMs, vm)
			vmInfos = append(vmInfos, vmInfo)
		}
	}

	// MCIS 생성
	logger.Infof("start create MCIS (name=%s)", mcis.Name)
	if err = mcis.POST(); err != nil {
		return nil, err
	}
	logger.Infof("create MCIS OK.. (name=%s)", mcis.Name)

	cpMcis := tumblebug.MCIS{}
	// 결과값 저장
	cluster.MCIS = mcis.Name
	for _, vm := range mcis.VMs {
		for _, vmInfo := range vmInfos {
			if vm.Name == vmInfo.Name {
				vm.Credential = vmInfo.Credential
				vm.Role = vmInfo.Role
				vm.Csp = vmInfo.Csp
				vm.IsCPLeader = vmInfo.IsCPLeader

				cpMcis.VMs = append(cpMcis.VMs, vm)
				break
			}
		}
	}

	return &cpMcis, nil
}

func bootstrapK8s(cluster *model.Cluster, mcis *tumblebug.MCIS) error {
	var wg sync.WaitGroup
	c := make(chan error)
	wg.Add(len(mcis.VMs))

	logger.Infoln("start k8s bootstrap")

	for _, vm := range mcis.VMs {
		go func(vm model.VM) {
			defer wg.Done()
			sshInfo := ssh.SSHInfo{
				UserName:   GetUserAccount(vm.Csp),
				PrivateKey: []byte(vm.Credential),
				ServerPort: fmt.Sprintf("%s:22", vm.PublicIP),
			}
			var err error

			// retry 100
			for i := 0; i < 100; i++ {
				err = vm.ConnectionTest(&sshInfo)
				if err == nil {
					break
				} else {
					time.Sleep(2 * time.Second)
					logger.Warnln("retry to connect vm(", vm.PublicIP, ")")
				}
			}

			if err != nil {
				c <- err
			}

			err = vm.CopyScripts(&sshInfo, cluster.NetworkCni)
			if err != nil {
				c <- err
			}

			err = vm.SetSystemd(&sshInfo, cluster.NetworkCni)
			if err != nil {
				c <- err
			}

			err = vm.Bootstrap(&sshInfo)
			if err != nil {
				c <- err
			}
		}(vm)
	}

	// FIXME: Call wg.Wait() in main go routine
	go func() {
		wg.Wait()
		close(c)
		logger.Infoln("end k8s bootstrap")
	}()

	for err := range c {
		if err != nil {
			return err
		}
	}

	return nil
}

func initandjoinK8s(cluster *model.Cluster, mcis *tumblebug.MCIS, cfg *model.Config) error {
	// init & join
	var joinCmd []string
	IPs := GetControlPlaneIPs(mcis.VMs)

	logger.Infoln("start k8s init")
	for _, vm := range mcis.VMs {
		if vm.Role == config.CONTROL_PLANE && vm.IsCPLeader {
			sshInfo := ssh.SSHInfo{
				UserName:   GetUserAccount(vm.Csp),
				PrivateKey: []byte(vm.Credential),
				ServerPort: fmt.Sprintf("%s:22", vm.PublicIP),
			}

			logger.Infof("install HAProxy (vm=%s)", vm.Name)
			err := vm.InstallHAProxy(&sshInfo, IPs)
			if err != nil {
				return err
			}

			logger.Infoln("control plane init")
			var clusterConfig string
			joinCmd, clusterConfig, err = vm.ControlPlaneInit(&sshInfo, cfg.Kubernetes)
			if err != nil {
				return err
			}
			cluster.ClusterConfig = clusterConfig

			logger.Infoln("install networkCNI")
			err = vm.InstallNetworkCNI(&sshInfo, cfg.Kubernetes.NetworkCni)
			if err != nil {
				return err
			}
		}
	}
	logger.Infoln("end k8s init")

	logger.Infoln("start k8s join")
	//
	// TODO: use go routine
	//
	for _, vm := range mcis.VMs {
		if vm.Role == config.CONTROL_PLANE && !vm.IsCPLeader {
			sshInfo := ssh.SSHInfo{
				UserName:   GetUserAccount(vm.Csp),
				PrivateKey: []byte(vm.Credential),
				ServerPort: fmt.Sprintf("%s:22", vm.PublicIP),
			}
			logger.Infof("control plane join (vm=%s)", vm.Name)
			err := vm.ControlPlaneJoin(&sshInfo, &joinCmd[0])
			if err != nil {
				return err
			}
		}
	}

	//
	// TODO: use go routine
	//
	for _, vm := range mcis.VMs {
		if vm.Role == config.WORKER {
			sshInfo := ssh.SSHInfo{
				UserName:   GetUserAccount(vm.Csp),
				PrivateKey: []byte(vm.Credential),
				ServerPort: fmt.Sprintf("%s:22", vm.PublicIP),
			}
			logger.Infof("worker join (vm=%s)", vm.Name)
			err := vm.WorkerJoin(&sshInfo, &joinCmd[1])
			if err != nil {
				return err
			}
		}
	}
	logger.Infoln("end k8s join")

	return nil
}

func CreateCluster(namespace string, req *model.ClusterReq) (*model.Cluster, error) {
	clusterName := req.Name
	cluster := model.NewCluster(namespace, clusterName)
	cluster.UId = lang.GetUid()
	cluster.NetworkCni = req.Config.Kubernetes.NetworkCni

	cpMcis, err := createMCIS(cluster, &req.ControlPlane, &req.Worker)
	if err != nil {
		return nil, err
	}

	var nodes []model.Node
	for _, vm := range cpMcis.VMs {
		node := model.NewNodeVM(cluster.Namespace, cluster.Name, vm)
		node.UId = lang.GetUid()

		// insert node in store
		nodes = append(nodes, *node)
		//nodeKey = lang.GetStoreMCKSNodeKey(cluster.Namespace, cluster.Name, node.Name)
		err := node.Insert()
		if err != nil {
			return nil, err
		}
	}

	clusterKey := lang.GetStoreMCKSClusterKey(cluster.Namespace, cluster.Name)

	err = cluster.Insert(clusterKey)
	if err != nil {
		return nil, err
	}

	time.Sleep(2 * time.Second)

	err = cluster.Update(clusterKey)
	if err != nil {
		return nil, err
	}

	// bootstrap
	if err = bootstrapK8s(cluster, cpMcis); err != nil {
		err_status := cluster.Fail(clusterKey)
		if err_status != nil {
			return nil, err_status
		}

		// TODO: not decided yet. Cleaning up resources related with MCIS

		return nil, err
	}

	if err = initandjoinK8s(cluster, cpMcis, &req.Config); err != nil {
		err_status := cluster.Fail(clusterKey)
		if err_status != nil {
			return nil, err_status
		}

		// TODO: not decided yet. Cleaning up resources related with MCIS

		return nil, err
	}

	cluster.Complete(clusterKey)
	cluster.Nodes = nodes

	return cluster, nil
}

func deleteMCIS(namespace string, clusterName string) (string, error) {
	msg := string("")
	mcisName := clusterName

	logger.Infof("start delete Cluster (name=%s)", mcisName)
	mcis := tumblebug.NewMCIS(namespace, mcisName)
	exist, err := mcis.GET()
	if err != nil {
		return msg, err
	}
	if exist {
		logger.Infof("terminate MCIS (name=%s)", mcisName)
		if err = mcis.TERMINATE(); err != nil {
			logger.Errorf("terminate mcis error : %v", err)
			return msg, err
		}
		time.Sleep(5 * time.Second)

		logger.Infof("delete MCIS (name=%s)", mcisName)
		if err = mcis.DELETE(); err != nil {
			if strings.Contains(err.Error(), "Deletion is not allowed") {
				logger.Infof("refine mcis (name=%s)", mcisName)
				if err = mcis.REFINE(); err != nil {
					logger.Errorf("refine MCIS error : %v", err)
					return msg, err
				}
				logger.Infof("delete MCIS (name=%s)", mcisName)
				if err = mcis.DELETE(); err != nil {
					logger.Errorf("delete MCIS error : %v", err)
					return msg, err
				}
			} else {
				logger.Errorf("delete MCIS error : %v", err)
				return msg, err
			}
		}

		logger.Infof("delete MCIS OK.. (name=%s)", mcisName)
		msg = fmt.Sprintf("cluster %s has been deleted", mcisName)
	} else {
		logger.Infof("delete Cluster skip (MCIS cannot find).. (name=%s)", mcisName)
		msg = fmt.Sprintf("cluster %s not found", mcisName)
	}

	return msg, nil
}

func DeleteCluster(namespace string, clusterName string) (*model.Status, error) {
	status := model.NewStatus()
	status.Code = model.STATUS_UNKNOWN

	msg, err := deleteMCIS(namespace, clusterName)
	if err != nil {
		return status, err
	}
	status.Code = model.STATUS_SUCCESS

	cluster := model.NewCluster(namespace, clusterName)
	clusterKey := lang.GetStoreMCKSClusterKey(namespace, clusterName)
	if err := cluster.Delete(clusterKey); err != nil {
		status.Message = fmt.Sprintf("%s. It cannot delete from the store", msg)
		return status, err
	}

	return status, nil
}
