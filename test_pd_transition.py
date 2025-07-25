#!/usr/bin/env python3
"""
PD节点角色转换系统测试脚本
"""

import asyncio
import json
import time
from typing import Dict, List

import aiohttp


class PDTransitionTester:
    """PD节点角色转换测试器"""
    
    def __init__(self, lb_url: str = "http://localhost:8000"):
        self.lb_url = lb_url
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def test_deployment_status(self):
        """测试部署状态查询"""
        print("🔍 测试部署状态查询...")
        try:
            async with self.session.get(f"{self.lb_url}/deployment_status") as response:
                if response.status == 200:
                    data = await response.json()
                    print("✅ 部署状态查询成功:")
                    print(f"   Prefill节点数: {data['prefill_count']}")
                    print(f"   Decode节点数: {data['decode_count']}")
                    print(f"   监控状态: {data['monitoring_enabled']}")
                    print("   节点详情:")
                    for url, info in data['nodes'].items():
                        print(f"     {url}: {info['current_role']} "
                             f"{'(转换中)' if info['is_transitioning'] else ''}")
                    return data
                else:
                    print(f"❌ 状态查询失败: {response.status}")
                    return None
        except Exception as e:
            print(f"❌ 状态查询出错: {e}")
            return None
    
    async def test_node_control(self, node_url: str, target_role: str):
        """测试节点角色转换控制"""
        print(f"🔄 测试节点角色转换: {node_url} -> {target_role}")
        try:
            payload = {
                "node_url": node_url,
                "target_role": target_role
            }
            async with self.session.post(
                f"{self.lb_url}/switch_node_role", 
                json=payload
            ) as response:
                data = await response.json()
                if response.status == 200:
                    print(f"✅ 角色转换成功: {data.get('message', '')}")
                    return True
                else:
                    print(f"❌ 角色转换失败: {data.get('error', response.status)}")
                    return False
        except Exception as e:
            print(f"❌ 角色转换出错: {e}")
            return False
    
    async def test_monitoring_toggle(self, enable: bool):
        """测试监控开关"""
        action = "启用" if enable else "禁用"
        print(f"⚙️  测试{action}负载监控...")
        try:
            payload = {"enable": enable}
            async with self.session.post(
                f"{self.lb_url}/toggle_monitoring",
                json=payload
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    print(f"✅ 监控{action}成功: {data.get('message', '')}")
                    return True
                else:
                    print(f"❌ 监控{action}失败: {response.status}")
                    return False
        except Exception as e:
            print(f"❌ 监控{action}出错: {e}")
            return False
    
    async def test_node_load_info(self, node_url: str):
        """测试节点负载信息获取"""
        print(f"📊 测试节点负载信息: {node_url}")
        try:
            async with self.session.get(f"{node_url}/get_role") as response:
                if response.status == 200:
                    data = await response.json()
                    print("✅ 节点信息获取成功:")
                    print(f"   当前角色: {data.get('current_role', 'unknown')}")
                    print(f"   负载信息: {data.get('load_info', {})}")
                    return data
                else:
                    print(f"❌ 节点信息获取失败: {response.status}")
                    return None
        except Exception as e:
            print(f"❌ 节点信息获取出错: {e}")
            return None
    
    async def test_direct_node_switch(self, node_url: str, target_role: str):
        """测试直接向节点发送角色转换请求"""
        print(f"🎯 测试直接节点转换: {node_url} -> {target_role}")
        try:
            payload = {"target_role": target_role}
            async with self.session.post(
                f"{node_url}/switch_role",
                json=payload
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    print(f"✅ 直接转换成功: {data.get('message', '')}")
                    return True
                else:
                    data = await response.json()
                    print(f"❌ 直接转换失败: {data.get('error', response.status)}")
                    return False
        except Exception as e:
            print(f"❌ 直接转换出错: {e}")
            return False
    
    async def run_comprehensive_test(self, test_nodes: List[str]):
        """运行综合测试"""
        print("🚀 开始PD节点角色转换综合测试\n")
        
        # 1. 查询初始状态
        print("=" * 50)
        initial_status = await self.test_deployment_status()
        if not initial_status:
            print("❌ 无法获取初始状态，测试终止")
            return
        
        # 2. 测试监控功能
        print("\n" + "=" * 50)
        await self.test_monitoring_toggle(False)
        await asyncio.sleep(1)
        await self.test_monitoring_toggle(True)
        
        # 3. 测试节点信息获取
        print("\n" + "=" * 50)
        for node_url in test_nodes:
            await self.test_node_load_info(node_url)
            await asyncio.sleep(1)
        
        # 4. 测试角色转换（通过LB）
        print("\n" + "=" * 50)
        if test_nodes:
            test_node = test_nodes[0]
            current_role = initial_status['nodes'][test_node]['current_role']
            target_role = "decode" if current_role == "prefill" else "prefill"
            
            print(f"准备将 {test_node} 从 {current_role} 转换为 {target_role}")
            success = await self.test_node_control(test_node, target_role)
            
            if success:
                await asyncio.sleep(2)
                print("检查转换后状态...")
                await self.test_deployment_status()
                
                # 转换回原角色
                print(f"\n准备将 {test_node} 转换回 {current_role}")
                await self.test_node_control(test_node, current_role)
                await asyncio.sleep(2)
                await self.test_deployment_status()
        
        # 5. 测试直接节点转换
        print("\n" + "=" * 50)
        if test_nodes:
            test_node = test_nodes[0]
            await self.test_direct_node_switch(test_node, "prefill")
            await asyncio.sleep(1)
            await self.test_direct_node_switch(test_node, "decode")
            await asyncio.sleep(1)
            await self.test_direct_node_switch(test_node, "prefill")
        
        print("\n🎉 综合测试完成！")


async def main():
    """主函数"""
    # 测试配置
    lb_url = "http://localhost:8000"
    test_nodes = [
        "http://localhost:30000",  # 示例节点1
        "http://localhost:30001",  # 示例节点2
    ]
    
    print("PD节点角色转换系统测试")
    print(f"负载均衡器: {lb_url}")
    print(f"测试节点: {test_nodes}")
    print()
    
    async with PDTransitionTester(lb_url) as tester:
        await tester.run_comprehensive_test(test_nodes)


if __name__ == "__main__":
    asyncio.run(main())
