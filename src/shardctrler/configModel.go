package shardctrler

import (
	"sort"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (cfg *Config) DeepCopy() Config {
	ret := Config{
		Num:    cfg.Num,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}

	for k, v := range cfg.Groups {
		ret.Groups[k] = v
	}
	for i := range cfg.Shards {
		ret.Shards[i] = cfg.Shards[i]
	}
	return ret
}

// --------------------------------------------------------------------- //

const magicNullGid = 0

type ConfigModel struct {
	configs []Config // indexed by config num
	me      int      // for debug
}

func NewConfigModel(me int) *ConfigModel {
	cfg := ConfigModel{make([]Config, 1), me}
	cfg.configs[0] = Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
	for i := range cfg.configs[0].Shards {
		cfg.configs[0].Shards[i] = magicNullGid
	}
	return &cfg
}

func (cm *ConfigModel) getGroup2Shards(config *Config) map[int][]int {
	group2shard := map[int][]int{}
	for gid, _ := range config.Groups {
		group2shard[gid] = []int{}
	}
	group2shard[magicNullGid] = []int{}

	for shard, gid := range config.Shards {
		group2shard[gid] = append(group2shard[gid], shard)
	}
	return group2shard
}

func (cm *ConfigModel) getMinShards(group2shard map[int][]int) int {
	var keys []int
	for k := range group2shard {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	gidRet, minn := -1, NShards+1
	for _, gid := range keys {
		if gid != magicNullGid && len(group2shard[gid]) < minn {
			gidRet, minn = gid, len(group2shard[gid])
		}
	}
	if gidRet == -1 {
		return magicNullGid
	}
	return gidRet
}

func (cm *ConfigModel) getMaxShards(group2shard map[int][]int) int {
	if shards, ok := group2shard[magicNullGid]; ok && len(shards) > 0 {
		return magicNullGid
	}

	var keys []int
	for k := range group2shard {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	gidRet, maxn := -1, -1
	for _, gid := range keys {
		if len(group2shard[gid]) > maxn {
			gidRet, maxn = gid, len(group2shard[gid])
		}
	}
	return gidRet
}

func (cm *ConfigModel) reBalance(config *Config) {
	// special judge
	if len(config.Groups) == 0 { // if none group, init shards
		for i := range config.Shards {
			config.Shards[i] = 0
		}
		return
	}

	// 1 shard - 1 group, 1 group - n shards
	group2shard := cm.getGroup2Shards(config)
	for {
		src := cm.getMaxShards(group2shard)
		dst := cm.getMinShards(group2shard)
		if src != magicNullGid && len(group2shard[src])-len(group2shard[dst]) <= 1 {
			break
		}

		group2shard[dst] = append(group2shard[dst], group2shard[src][0])
		group2shard[src] = group2shard[src][1:]
	}

	// reset shard
	for gid, shards := range group2shard {
		for _, shard := range shards {
			config.Shards[shard] = gid
		}
	}
}

func (cm *ConfigModel) join(servers map[int][]string) Err {
	newConfig := cm.configs[len(cm.configs)-1].DeepCopy()
	newConfig.Num = len(cm.configs)

	for gid, servers_iter := range servers {
		newServers := make([]string, len(servers_iter))
		copy(newServers, servers_iter)
		if _, ok := newConfig.Groups[gid]; !ok {
			newConfig.Groups[gid] = newServers
		} else {
			newConfig.Groups[gid] = append(newConfig.Groups[gid], newServers...)
		}
	}

	cm.reBalance(&newConfig)
	cm.configs = append(cm.configs, newConfig)
	return OK
}

func (cm *ConfigModel) leave(GIDs []int) Err {
	newConfig := cm.configs[len(cm.configs)-1].DeepCopy()
	newConfig.Num = len(cm.configs)

	group2shard := cm.getGroup2Shards(&newConfig)
	for _, gid := range GIDs {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := group2shard[gid]; ok {
			for _, shard := range shards {
				newConfig.Shards[shard] = magicNullGid
			}
		}
	}

	cm.reBalance(&newConfig)
	cm.configs = append(cm.configs, newConfig)
	return OK
}

func (cm *ConfigModel) move(shard int, gid int) Err {
	newConfig := cm.configs[len(cm.configs)-1].DeepCopy()
	newConfig.Num = len(cm.configs)
	newConfig.Shards[shard] = gid
	cm.configs = append(cm.configs, newConfig)
	return OK
}

func (cm *ConfigModel) query(num int) (Config, Err) {
	if num < 0 || num >= len(cm.configs) {
		return cm.configs[len(cm.configs)-1].DeepCopy(), OK
	}
	return cm.configs[num].DeepCopy(), OK
}

func (cm *ConfigModel) isLegal(opType OpType) bool {
	switch opType {
	case OpJoin:
	case OpLeave:
	case OpMove:
	case OpQuery:
	default:
		return false
	}
	return true
}

func (cm *ConfigModel) Opt(cmd Op) (Config, Err) {
	switch cmd.Op {
	case OpJoin:
		err := cm.join(cmd.Servers)
		return Config{}, err
	case OpLeave:
		err := cm.leave(cmd.GIDs)
		return Config{}, err
	case OpMove:
		err := cm.move(cmd.Shard, cmd.GID)
		return Config{}, err
	case OpQuery:
		config, err := cm.query(cmd.Num)
		return config, err
	default:
		return Config{}, ErrOpt
	}
}
