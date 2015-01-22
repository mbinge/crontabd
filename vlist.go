package main

// #include <stdlib.h>
// #include <stdio.h>
// #include <string.h>
//
// void* node_init()
// {
// 		return malloc(1024*1024*200);
// }
// int node_append(void *node, int offset, void *data, int len)
// {
//		memcpy(node+offset, &len, sizeof(len));
//      memcpy(node+offset+sizeof(len), data, len);
//      return offset+sizeof(len)+len;
// }
// void* node_data(void *node, int offset)
// {
//      return node+offset+sizeof(offset);
// }
// int node_data_len(void *node, int offset)
// {
//		int len = 0;
//		memcpy(&len, node+offset, sizeof(len));
// 		return len;
// }
// int node_offset(int old, int len){
//		return old + len + sizeof(len);
// }
// void node_drop(void *node)
// {
//      free(node);
// }
import "C"
import "unsafe"

type vSlice struct {
	cur_offset C.int
	cut_offset C.int
	root_node  unsafe.Pointer
	total      int
}

func (vs *vSlice) Add(bytes []byte) {
	if vs.root_node == nil {
		vs.root_node = C.node_init()
		vs.cur_offset = 0
		vs.cut_offset = 0
		vs.total = 0
	}
	vs.cur_offset = C.node_append(vs.root_node, vs.cur_offset, unsafe.Pointer(&bytes[0]), C.int(len(bytes)))
	bytes = nil
	vs.total = vs.total + 1
}

func (vs *vSlice) Cut() []byte {
	dlen := C.node_data_len(vs.root_node, vs.cut_offset)
	dp := C.node_data(vs.root_node, vs.cut_offset)
	ret := C.GoBytes(dp, dlen)
	vs.cut_offset = C.node_offset(vs.cut_offset, dlen)
	vs.total = vs.total - 1
	return ret
}

func (vs *vSlice) Drop() {
	C.node_drop(vs.root_node)
}

type vList struct {
	list   []*vSlice
	offset int64
}

func (vl *vList) Init() {
	vl.list = make([]*vSlice, 0)
	vl.offset = 0
}

func (vl *vList) Set(key int64, value []byte) {
	if vl.offset == 0 {
		vl.offset = key
	}
	map_index := key - vl.offset
	if map_index < 0 {
		return
	}
	cur_len := int64(len(vl.list))
	new_len := map_index - cur_len + 10
	for i := int64(0); i < new_len; i++ {
		vslice := new(vSlice)
		vl.list = append(vl.list, vslice)
	}
	vl.list[map_index].Add(value)
}

func (vl *vList) Pop(key int64) *vSlice {
	var last *vSlice
	map_index := key - vl.offset + 1
	if int64(len(vl.list)) < map_index {
		return last
	}
	for i := int64(0); i < map_index; i++ {
		last = vl.list[0]
		vl.list[0] = nil
		vl.list = vl.list[1:]
		vl.offset = vl.offset + 1
	}
	return last
}
