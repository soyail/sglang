"""
Minimal HTTP load balancer for prefill and decode servers for testing.
"""

import asyncio
import dataclasses
import logging
import random
import threading
import time
import urllib
from itertools import chain
from typing import Dict, List, Optional

import aiohttp
import orjson
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import ORJSONResponse, Response, StreamingResponse

from sglang.srt.disaggregation.utils import PDRegistryRequest

AIOHTTP_STREAM_READ_CHUNK_SIZE = (
    1024 * 64
)  # 64KB, to prevent aiohttp's "Chunk too big" error


def setup_logger():
    logger = logging.getLogger("pdlb")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "[PDLB (Python)] %(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


logger = setup_logger()


@dataclasses.dataclass
class PrefillConfig:
    url: str
    bootstrap_port: Optional[int] = None


@dataclasses.dataclass
class NodeConfig:
    """统一的节点配置，支持动态角色转换"""
    url: str
    current_role: str  # "prefill" or "decode"
    bootstrap_port: Optional[int] = None
    last_load: float = 0.0
    last_update_time: float = 0.0
    is_transitioning: bool = False


@dataclasses.dataclass
class LoadMetrics:
    """节点负载指标"""
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    request_queue_size: int = 0
    throughput: float = 0.0
    avg_latency: float = 0.0


class MiniLoadBalancer:
    def __init__(self, prefill_configs: List[PrefillConfig], decode_servers: List[str]):
        self.prefill_configs = prefill_configs
        self.prefill_servers = [p.url for p in prefill_configs]
        self.decode_servers = decode_servers
        
        # 新增：统一节点管理
        self.nodes: Dict[str, NodeConfig] = {}
        self._init_nodes()
        
        # 负载监控配置
        self.load_check_interval = 300.0  # 300秒检查一次负载
        self.load_imbalance_threshold = 0.7  # 负载不均衡阈值
        self.monitoring_enabled = True
        self.monitoring_thread = None
        
        # 启动负载监控线程
        self._start_load_monitoring()

    def _init_nodes(self):
        """初始化节点配置"""
        for config in self.prefill_configs:
            self.nodes[config.url] = NodeConfig(
                url=config.url,
                current_role="prefill",
                bootstrap_port=config.bootstrap_port
            )
        
        for server in self.decode_servers:
            self.nodes[server] = NodeConfig(
                url=server,
                current_role="decode"
            )

    def _start_load_monitoring(self):
        """启动负载监控线程"""
        if self.monitoring_thread is None:
            self.monitoring_enabled = True
            self.monitoring_thread = threading.Thread(
                target=self._load_monitoring_loop,
                daemon=True
            )
            self.monitoring_thread.start()
            logger.info("负载监控线程已启动")

    def _stop_load_monitoring(self):
        """停止负载监控线程"""
        self.monitoring_enabled = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5.0)
            self.monitoring_thread = None
            logger.info("负载监控线程已停止")

    def _load_monitoring_loop(self):
        """负载监控主循环"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            while self.monitoring_enabled:
                try:
                    loop.run_until_complete(self._check_and_balance_load())
                except Exception as e:
                    logger.error(f"负载监控出错: {e}")
                
                # 等待下一次检查
                for _ in range(int(self.load_check_interval)):
                    if not self.monitoring_enabled:
                        break
                    time.sleep(1.0)
        finally:
            loop.close()

    async def _check_and_balance_load(self):
        """检查负载并执行负载均衡"""
        try:
            # 收集所有节点的负载信息
            load_metrics = await self._collect_load_metrics()
            
            # 分析负载是否均衡
            imbalance_info = self._analyze_load_imbalance(load_metrics)
            
            if imbalance_info:
                logger.info(f"检测到负载不均衡: {imbalance_info}")
                # 执行角色转换
                await self._execute_role_transitions(imbalance_info)
                
        except Exception as e:
            logger.error(f"负载检查失败: {e}")

    async def _collect_load_metrics(self) -> Dict[str, LoadMetrics]:
        """收集所有节点的负载指标"""
        load_metrics = {}
        
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            tasks = []
            for node_url in self.nodes.keys():
                tasks.append(self._get_node_load(session, node_url))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for node_url, result in zip(self.nodes.keys(), results):
                if isinstance(result, Exception):
                    logger.warning(f"获取节点 {node_url} 负载失败: {result}")
                    # 使用默认负载指标
                    load_metrics[node_url] = LoadMetrics()
                else:
                    load_metrics[node_url] = result
                    
        return load_metrics

    async def _get_node_load(self, session: aiohttp.ClientSession, node_url: str) -> LoadMetrics:
        """获取单个节点的负载"""
        try:
            async with session.get(f"{node_url}/get_load") as response:
                if response.status == 200:
                    data = await response.json()
                    return LoadMetrics(
                        cpu_usage=data.get("cpu_usage", 0.0),
                        memory_usage=data.get("memory_usage", 0.0),
                        request_queue_size=data.get("queue_size", 0),
                        throughput=data.get("throughput", 0.0),
                        avg_latency=data.get("avg_latency", 0.0)
                    )
        except Exception as e:
            logger.warning(f"获取节点 {node_url} 负载指标失败: {e}")
        
        return LoadMetrics()

    def _analyze_load_imbalance(self, load_metrics: Dict[str, LoadMetrics]) -> Optional[Dict]:
        """分析负载是否不均衡"""
        prefill_nodes = [url for url, node in self.nodes.items() if node.current_role == "prefill"]
        decode_nodes = [url for url, node in self.nodes.items() if node.current_role == "decode"]
        
        if not prefill_nodes or not decode_nodes:
            return None
            
        # 计算平均负载
        prefill_avg_load = sum(load_metrics[url].cpu_usage for url in prefill_nodes) / len(prefill_nodes)
        decode_avg_load = sum(load_metrics[url].cpu_usage for url in decode_nodes) / len(decode_nodes)
        
        # 检查是否存在负载不均衡
        load_diff = abs(prefill_avg_load - decode_avg_load)
        
        if load_diff > self.load_imbalance_threshold:
            # 确定转换方向
            if prefill_avg_load > decode_avg_load:
                # prefill负载高，需要将一些prefill节点转为decode
                heavy_role = "prefill"
                light_role = "decode"
                heavy_nodes = prefill_nodes
                light_nodes = decode_nodes
            else:
                # decode负载高，需要将一些decode节点转为prefill
                heavy_role = "decode" 
                light_role = "prefill"
                heavy_nodes = decode_nodes
                light_nodes = prefill_nodes
                
            # 选择负载最高的节点进行转换
            heavy_node_loads = [(url, load_metrics[url].cpu_usage) for url in heavy_nodes]
            heavy_node_loads.sort(key=lambda x: x[1], reverse=True)
            
            return {
                "heavy_role": heavy_role,
                "light_role": light_role,
                "candidate_node": heavy_node_loads[0][0],
                "load_diff": load_diff,
                "heavy_avg_load": prefill_avg_load if heavy_role == "prefill" else decode_avg_load,
                "light_avg_load": decode_avg_load if heavy_role == "prefill" else prefill_avg_load
            }
            
        return None

    async def _execute_role_transitions(self, imbalance_info: Dict):
        """执行角色转换"""
        candidate_node = imbalance_info["candidate_node"]
        target_role = imbalance_info["light_role"]
        
        logger.info(f"开始将节点 {candidate_node} 从 {imbalance_info['heavy_role']} 转换为 {target_role}")
        
        try:
            # 调用节点的角色转换API
            success = await self._switch_node_role(candidate_node, target_role)
            
            if success:
                # 更新本地节点配置
                self.nodes[candidate_node].current_role = target_role
                self.nodes[candidate_node].is_transitioning = False
                
                # 更新服务器列表
                self._update_server_lists()
                
                logger.info(f"节点 {candidate_node} 成功转换为 {target_role}")
            else:
                logger.error(f"节点 {candidate_node} 角色转换失败")
                
        except Exception as e:
            logger.error(f"执行节点 {candidate_node} 角色转换时出错: {e}")

    async def _switch_node_role(self, node_url: str, target_role: str) -> bool:
        """调用节点的角色转换API"""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                payload = {"target_role": target_role}
                async with session.post(f"{node_url}/switch_role", json=payload) as response:
                    return response.status == 200
        except Exception as e:
            logger.error(f"调用节点 {node_url} 角色转换API失败: {e}")
            return False

    def _update_server_lists(self):
        """根据当前节点角色更新服务器列表"""
        self.prefill_servers = []
        self.prefill_configs = []
        self.decode_servers = []
        
        for url, node in self.nodes.items():
            if node.current_role == "prefill":
                self.prefill_servers.append(url)
                self.prefill_configs.append(PrefillConfig(url, node.bootstrap_port))
            elif node.current_role == "decode":
                self.decode_servers.append(url)

    def add_prefill_server(self, new_prefill_config: PrefillConfig):
        self.prefill_configs.append(new_prefill_config)
        self.prefill_servers.append(new_prefill_config.url)
        
        # 添加到统一节点管理
        self.nodes[new_prefill_config.url] = NodeConfig(
            url=new_prefill_config.url,
            current_role="prefill",
            bootstrap_port=new_prefill_config.bootstrap_port
        )

    def add_decode_server(self, new_decode_server: str):
        self.decode_servers.append(new_decode_server)
        
        # 添加到统一节点管理
        self.nodes[new_decode_server] = NodeConfig(
            url=new_decode_server,
            current_role="decode"
        )

    def select_pair(self):
        # TODO: return some message instead of panic
        assert len(self.prefill_configs) > 0, "No prefill servers available"
        assert len(self.decode_servers) > 0, "No decode servers available"

        prefill_config = random.choice(self.prefill_configs)
        decode_server = random.choice(self.decode_servers)
        return prefill_config.url, prefill_config.bootstrap_port, decode_server

    async def generate(
        self, modified_request, prefill_server, decode_server, endpoint
    ) -> ORJSONResponse:
        assert endpoint[0] != "/", f"Endpoint should not start with '/': {endpoint}"

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(
                total=3600
            )  # Add timeout for request reliability
        ) as session:
            tasks = [
                session.post(f"{prefill_server}/{endpoint}", json=modified_request),
                session.post(f"{decode_server}/{endpoint}", json=modified_request),
            ]

            # Wait for both responses to complete. Prefill should end first.
            prefill_response, decode_response = await asyncio.gather(*tasks)

            if "return_logprob" in modified_request:

                prefill_json = await prefill_response.json()
                ret_json = await decode_response.json()

                # merge `meta_info.input_token_logprobs` from prefill to decode
                if "meta_info" in ret_json:
                    if "input_token_logprobs" in ret_json["meta_info"]:
                        ret_json["meta_info"]["input_token_logprobs"] = (
                            prefill_json["meta_info"]["input_token_logprobs"]
                            + ret_json["meta_info"]["input_token_logprobs"]
                        )
            else:
                ret_json = await decode_response.json()

            return ORJSONResponse(
                content=ret_json,
                status_code=decode_response.status,
            )

    async def generate_stream(
        self, modified_request, prefill_server, decode_server, endpoint="generate"
    ):
        assert endpoint[0] != "/", f"Endpoint should not start with '/': {endpoint}"

        async def stream_results():
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(
                    total=3600
                )  # Add timeout for request reliability
            ) as session:
                # Create the tasks for both prefill and decode requests
                tasks = [
                    session.post(f"{prefill_server}/{endpoint}", json=modified_request),
                    session.post(f"{decode_server}/{endpoint}", json=modified_request),
                ]
                # Wait for both responses to complete. Since this is streaming, they return immediately.
                prefill_response, decode_response = await asyncio.gather(*tasks)

                if modified_request.get("return_logprob", False):
                    prefill_chunks = []
                    async for chunk in prefill_response.content:
                        prefill_chunks.append(chunk)

                    first_prefill_chunk = (
                        prefill_chunks[0].decode("utf-8")[5:].strip("\n")
                    )
                    first_prefill_chunk_json = orjson.loads(first_prefill_chunk)

                    async for chunk in decode_response.content:
                        # Note: This is inefficient
                        # merge prefill input_token_logprobs, output_token_logprobs to decode
                        decoded_chunk = chunk.decode("utf-8")
                        if (
                            decoded_chunk
                            and decoded_chunk.startswith("data:")
                            and "[DONE]" not in decoded_chunk
                        ):
                            ret_json = orjson.loads(decoded_chunk[5:].strip("\n"))
                            ret_json["meta_info"]["input_token_logprobs"] = (
                                first_prefill_chunk_json["meta_info"][
                                    "input_token_logprobs"
                                ]
                                + ret_json["meta_info"]["input_token_logprobs"]
                            )

                            yield b"data: " + orjson.dumps(ret_json) + b"\n\n"
                        else:
                            yield chunk
                else:
                    async for chunk in decode_response.content.iter_chunked(
                        AIOHTTP_STREAM_READ_CHUNK_SIZE
                    ):
                        yield chunk

        return StreamingResponse(
            stream_results(),
            media_type="text/event-stream",
        )


app = FastAPI()
load_balancer: Optional[MiniLoadBalancer] = None


@app.get("/health")
async def health_check():
    return Response(status_code=200)


@app.get("/health_generate")
async def health_check():
    prefill_servers, decode_servers = (
        load_balancer.prefill_servers,
        load_balancer.decode_servers,
    )
    async with aiohttp.ClientSession() as session:
        # Create the tasks
        tasks = []
        for server in chain(prefill_servers, decode_servers):
            tasks.append(session.post(f"{server}/health_generate"))
        for i, response in enumerate(asyncio.as_completed(tasks)):
            await response
    return Response(status_code=200)


@app.post("/flush_cache")
async def flush_cache():
    prefill_servers, decode_servers = (
        load_balancer.prefill_servers,
        load_balancer.decode_servers,
    )
    async with aiohttp.ClientSession() as session:
        # Create the tasks
        tasks = []
        for server in chain(prefill_servers, decode_servers):
            tasks.append(session.post(f"{server}/flush_cache"))
        for i, response in enumerate(asyncio.as_completed(tasks)):
            await response
    return Response(status_code=200)


@app.get("/get_server_info")
async def get_server_info():
    prefill_servers, decode_servers = (
        load_balancer.prefill_servers,
        load_balancer.decode_servers,
    )
    prefill_infos = []
    decode_infos = []
    all_internal_states = []

    async with aiohttp.ClientSession() as session:
        for server in chain(prefill_servers):
            server_info = await session.get(f"{server}/get_server_info")
            prefill_infos.append(await server_info.json())
        for server in chain(decode_servers):
            server_info = await session.get(f"{server}/get_server_info")
            info_json = await server_info.json()
            decode_infos.append(info_json)
            # Extract internal_states from decode servers
            if "internal_states" in info_json:
                all_internal_states.extend(info_json["internal_states"])

    # Return format expected by bench_one_batch_server.py
    if all_internal_states:
        return {
            "internal_states": all_internal_states,
            "prefill": prefill_infos,
            "decode": decode_infos,
        }
    else:
        # Fallback with dummy data if no internal states found
        return {
            "internal_states": [
                {
                    "last_gen_throughput": 0.0,
                    "avg_spec_accept_length": None,
                }
            ],
            "prefill": prefill_infos,
            "decode": decode_infos,
        }


@app.get("/get_model_info")
async def get_model_info():
    # Dummy model information
    model_info = {
        "model_path": "/path/to/dummy/model",
        "tokenizer_path": "/path/to/dummy/tokenizer",
        "is_generation": True,
        "preferred_sampling_params": {"temperature": 0.7, "max_new_tokens": 128},
    }
    return ORJSONResponse(content=model_info)


@app.post("/generate")
async def handle_generate_request(request_data: dict):
    prefill_server, bootstrap_port, decode_server = load_balancer.select_pair()

    # Parse and transform prefill_server for bootstrap data
    parsed_url = urllib.parse.urlparse(prefill_server)
    hostname = parsed_url.hostname
    modified_request = request_data.copy()

    batch_size = _get_request_batch_size(modified_request)
    if batch_size is not None:
        modified_request.update(
            {
                "bootstrap_host": [hostname] * batch_size,
                "bootstrap_port": [bootstrap_port] * batch_size,
                "bootstrap_room": [
                    _generate_bootstrap_room() for _ in range(batch_size)
                ],
            }
        )
    else:
        modified_request.update(
            {
                "bootstrap_host": hostname,
                "bootstrap_port": bootstrap_port,
                "bootstrap_room": _generate_bootstrap_room(),
            }
        )

    if request_data.get("stream", False):
        return await load_balancer.generate_stream(
            modified_request, prefill_server, decode_server, "generate"
        )
    else:
        return await load_balancer.generate(
            modified_request, prefill_server, decode_server, "generate"
        )


async def _forward_to_backend(request_data: dict, endpoint_name: str):
    prefill_server, bootstrap_port, decode_server = load_balancer.select_pair()

    # Parse and transform prefill_server for bootstrap data
    parsed_url = urllib.parse.urlparse(prefill_server)
    hostname = parsed_url.hostname
    modified_request = request_data.copy()
    modified_request.update(
        {
            "bootstrap_host": hostname,
            "bootstrap_port": bootstrap_port,
            "bootstrap_room": _generate_bootstrap_room(),
        }
    )

    if request_data.get("stream", False):
        return await load_balancer.generate_stream(
            modified_request,
            prefill_server,
            decode_server,
            endpoint=endpoint_name,
        )
    else:
        return await load_balancer.generate(
            modified_request,
            prefill_server,
            decode_server,
            endpoint=endpoint_name,
        )


@app.post("/v1/chat/completions")
async def handle_chat_completion_request(request_data: dict):
    return await _forward_to_backend(request_data, "v1/chat/completions")


@app.post("/v1/completions")
async def handle_completion_request(request_data: dict):
    return await _forward_to_backend(request_data, "v1/completions")


def _generate_bootstrap_room():
    return random.randint(0, 2**63 - 1)


# We may utilize `GenerateReqInput`'s logic later
def _get_request_batch_size(request):
    text = request.get("text")
    if text is not None:
        return None if isinstance(text, str) else len(text)
    input_ids = request.get("input_ids")
    if input_ids is not None:
        return None if isinstance(input_ids[0], int) else len(input_ids)
    return None


@app.get("/v1/models")
async def get_models():
    prefill_server = load_balancer.prefill_servers[0]  # Get the first prefill server
    async with aiohttp.ClientSession() as session:
        try:
            response = await session.get(f"{prefill_server}/v1/models")
            if response.status != 200:
                raise HTTPException(
                    status_code=response.status,
                    detail=f"Prefill server error: Status {response.status}",
                )
            return ORJSONResponse(content=await response.json())
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.post("/register")
async def register(obj: PDRegistryRequest):
    if obj.mode == "prefill":
        load_balancer.add_prefill_server(
            PrefillConfig(obj.registry_url, obj.bootstrap_port)
        )
        logger.info(
            f"Registered prefill server: {obj.registry_url} with bootstrap port: {obj.bootstrap_port}"
        )
    elif obj.mode == "decode":
        load_balancer.add_decode_server(obj.registry_url)
        logger.info(f"Registered decode server: {obj.registry_url}")
    else:
        raise HTTPException(
            status_code=400,
            detail="Invalid mode. Must be either PREFILL or DECODE.",
        )

    logger.info(
        f"#Prefill servers: {len(load_balancer.prefill_configs)}, "
        f"#Decode servers: {len(load_balancer.decode_servers)}"
    )

    return Response(status_code=200)


@app.get("/deployment_status")
async def get_deployment_status():
    """获取当前部署状态"""
    if not load_balancer:
        raise HTTPException(status_code=500, detail="Load balancer not initialized")
    
    status = {
        "nodes": {},
        "prefill_count": len(load_balancer.prefill_servers),
        "decode_count": len(load_balancer.decode_servers),
        "monitoring_enabled": load_balancer.monitoring_enabled
    }
    
    for url, node in load_balancer.nodes.items():
        status["nodes"][url] = {
            "current_role": node.current_role,
            "bootstrap_port": node.bootstrap_port,
            "last_load": node.last_load,
            "last_update_time": node.last_update_time,
            "is_transitioning": node.is_transitioning
        }
    
    return ORJSONResponse(content=status)


@app.post("/switch_node_role")
async def switch_node_role(request_data: dict):
    """手动切换节点角色"""
    if not load_balancer:
        raise HTTPException(status_code=500, detail="Load balancer not initialized")
    
    node_url = request_data.get("node_url")
    target_role = request_data.get("target_role")
    
    if not node_url or not target_role:
        raise HTTPException(status_code=400, detail="Missing node_url or target_role")
    
    if target_role not in ["prefill", "decode"]:
        raise HTTPException(status_code=400, detail="Invalid target_role. Must be 'prefill' or 'decode'")
    
    if node_url not in load_balancer.nodes:
        raise HTTPException(status_code=404, detail=f"Node {node_url} not found")
    
    current_role = load_balancer.nodes[node_url].current_role
    if current_role == target_role:
        return ORJSONResponse(content={"message": f"Node {node_url} is already {target_role}"})
    
    try:
        # 标记节点正在转换
        load_balancer.nodes[node_url].is_transitioning = True
        
        # 执行角色转换
        success = await load_balancer._switch_node_role(node_url, target_role)
        
        if success:
            # 更新节点配置
            load_balancer.nodes[node_url].current_role = target_role
            load_balancer.nodes[node_url].is_transitioning = False
            load_balancer._update_server_lists()
            
            return ORJSONResponse(content={
                "message": f"Successfully switched node {node_url} from {current_role} to {target_role}"
            })
        else:
            load_balancer.nodes[node_url].is_transitioning = False
            raise HTTPException(status_code=500, detail=f"Failed to switch node {node_url} role")
            
    except Exception as e:
        load_balancer.nodes[node_url].is_transitioning = False
        raise HTTPException(status_code=500, detail=f"Error switching node role: {str(e)}")


@app.post("/toggle_monitoring")
async def toggle_monitoring(request_data: dict):
    """启用/禁用负载监控"""
    if not load_balancer:
        raise HTTPException(status_code=500, detail="Load balancer not initialized")
    
    enable = request_data.get("enable", True)
    
    if enable and not load_balancer.monitoring_enabled:
        load_balancer._start_load_monitoring()
        message = "Load monitoring enabled"
    elif not enable and load_balancer.monitoring_enabled:
        load_balancer._stop_load_monitoring()
        message = "Load monitoring disabled"
    else:
        message = f"Load monitoring already {'enabled' if enable else 'disabled'}"
    
    return ORJSONResponse(content={"message": message, "monitoring_enabled": load_balancer.monitoring_enabled})


def run(prefill_configs, decode_addrs, host, port):
    global load_balancer
    load_balancer = MiniLoadBalancer(prefill_configs, decode_addrs)
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    # FIXME: remove this, use the unified entry point: sglang.srt.disaggregation.launch_lb
    from sglang.srt.disaggregation.launch_lb import main

    main()
