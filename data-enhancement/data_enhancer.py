#!/usr/bin/env python3
"""
企业数据增强工具
帮助学生改善数据集质量的辅助工具
"""

import pandas as pd
import json
import numpy as np
from typing import Dict, List, Any, Optional
from pathlib import Path


class EnterpriseDataEnhancer:
    """企业数据增强器"""
    
    def __init__(self):
        # 行业标杆数据
        self.industry_benchmarks = {
            "商贸零售": {
                "roe": 0.12,
                "roa": 0.08,
                "debt_ratio": 0.45,
                "employee_per_million_revenue": 25,
                "profit_margin": 0.08
            },
            "制造业": {
                "roe": 0.15,
                "roa": 0.10,
                "debt_ratio": 0.55,
                "employee_per_million_revenue": 15,
                "profit_margin": 0.12
            },
            "金融业": {
                "roe": 0.18,
                "roa": 0.06,
                "debt_ratio": 0.85,
                "employee_per_million_revenue": 8,
                "profit_margin": 0.25
            },
            "科技企业": {
                "roe": 0.20,
                "roa": 0.12,
                "debt_ratio": 0.35,
                "employee_per_million_revenue": 12,
                "profit_margin": 0.18
            },
            "交通运输": {
                "roe": 0.10,
                "roa": 0.05,
                "debt_ratio": 0.60,
                "employee_per_million_revenue": 20,
                "profit_margin": 0.06
            }
        }
    
    def classify_industry_group(self, industry_detail: str) -> str:
        """将详细行业分类映射到标杆组别"""
        if "零售" in industry_detail or "商贸" in industry_detail:
            return "商贸零售"
        elif "制造" in industry_detail or "机械" in industry_detail or "化工" in industry_detail:
            return "制造业"  
        elif "金融" in industry_detail or "证券" in industry_detail or "银行" in industry_detail:
            return "金融业"
        elif "科技" in industry_detail or "软件" in industry_detail or "互联网" in industry_detail:
            return "科技企业"
        elif "运输" in industry_detail or "航运" in industry_detail or "物流" in industry_detail:
            return "交通运输"
        else:
            return "商贸零售"  # 默认分组
    
    def estimate_employees(self, revenue: float, industry_group: str) -> int:
        """基于营收估算员工数量"""
        benchmark = self.industry_benchmarks.get(industry_group, self.industry_benchmarks["商贸零售"])
        employee_per_million = benchmark["employee_per_million_revenue"]
        revenue_in_millions = revenue / 1_000_000
        return max(int(revenue_in_millions * employee_per_million), 10)  # 最少10人
    
    def estimate_profit(self, revenue: float, industry_group: str) -> float:
        """基于营收估算利润"""
        benchmark = self.industry_benchmarks.get(industry_group, self.industry_benchmarks["商贸零售"])
        profit_margin = benchmark["profit_margin"]
        return revenue * profit_margin
    
    def estimate_financial_ratios(self, total_assets: float, net_profit: float, 
                                 revenue: float, industry_group: str) -> Dict[str, float]:
        """估算财务比率"""
        benchmark = self.industry_benchmarks.get(industry_group, self.industry_benchmarks["商贸零售"])
        
        ratios = {}
        
        # ROE (净资产收益率)
        if net_profit and total_assets:
            # 假设净资产 = 总资产 * (1 - 负债率)
            estimated_net_assets = total_assets * (1 - benchmark["debt_ratio"])
            ratios["roe"] = net_profit / estimated_net_assets if estimated_net_assets > 0 else benchmark["roe"]
        else:
            ratios["roe"] = benchmark["roe"]
        
        # ROA (总资产收益率)
        if net_profit and total_assets:
            ratios["roa"] = net_profit / total_assets
        else:
            ratios["roa"] = benchmark["roa"]
        
        # 负债率
        ratios["debt_ratio"] = benchmark["debt_ratio"]
        
        # 资产周转率
        if revenue and total_assets:
            ratios["total_asset_turnover"] = revenue / total_assets
        else:
            ratios["total_asset_turnover"] = 1.0
        
        return ratios
    
    def enhance_csv_data(self, input_file: str, output_file: str) -> Dict[str, Any]:
        """增强CSV格式的企业数据"""
        df = pd.read_csv(input_file)
        
        enhancement_log = {
            "original_missing": {},
            "enhanced_fields": [],
            "enhancement_methods": {}
        }
        
        # 分析缺失情况
        for col in df.columns:
            missing_count = df[col].isnull().sum()
            if missing_count > 0:
                enhancement_log["original_missing"][col] = int(missing_count)
        
        # 增强各字段
        for idx, row in df.iterrows():
            industry_group = self.classify_industry_group(row.get('industry_type', ''))
            
            # 增强员工数量
            if pd.isna(row.get('employee_count')) and not pd.isna(row.get('revenue')):
                estimated_employees = self.estimate_employees(row['revenue'], industry_group)
                df.loc[idx, 'employee_count'] = estimated_employees
                if 'employee_count' not in enhancement_log["enhanced_fields"]:
                    enhancement_log["enhanced_fields"].append('employee_count')
                    enhancement_log["enhancement_methods"]['employee_count'] = "基于营收和行业标杆估算"
            
            # 增强利润数据
            if pd.isna(row.get('net_profit')) and not pd.isna(row.get('revenue')):
                estimated_profit = self.estimate_profit(row['revenue'], industry_group)
                df.loc[idx, 'net_profit'] = estimated_profit
                if 'net_profit' not in enhancement_log["enhanced_fields"]:
                    enhancement_log["enhanced_fields"].append('net_profit')
                    enhancement_log["enhancement_methods"]['net_profit'] = "基于营收和行业利润率估算"
        
        # 保存增强后的数据
        df.to_csv(output_file, index=False)
        
        # 统计改善情况
        enhanced_missing = {}
        for col in df.columns:
            missing_count = df[col].isnull().sum()
            if missing_count > 0:
                enhanced_missing[col] = int(missing_count)
        
        enhancement_log["final_missing"] = enhanced_missing
        enhancement_log["total_records"] = len(df)
        enhancement_log["improvement_summary"] = {
            "fields_enhanced": len(enhancement_log["enhanced_fields"]),
            "missing_reduction": len(enhancement_log["original_missing"]) - len(enhanced_missing)
        }
        
        return enhancement_log

    def generate_enhancement_report(self, log: Dict[str, Any], output_file: str):
        """生成数据增强报告"""
        report = f"""# 数据增强报告

## 原始数据情况
总记录数：{log['total_records']}

### 缺失字段统计（增强前）
"""
        for field, count in log["original_missing"].items():
            percentage = (count / log['total_records']) * 100
            report += f"- {field}: {count}条缺失 ({percentage:.1f}%)\n"
        
        report += f"""
## 增强处理结果

### 增强的字段
"""
        for field in log["enhanced_fields"]:
            method = log["enhancement_methods"].get(field, "未知方法")
            report += f"- {field}: {method}\n"
        
        report += f"""
### 缺失字段统计（增强后）
"""
        for field, count in log["final_missing"].items():
            percentage = (count / log['total_records']) * 100
            report += f"- {field}: {count}条缺失 ({percentage:.1f}%)\n"
        
        report += f"""
## 改善总结
- 增强字段数量: {log['improvement_summary']['fields_enhanced']}
- 数据完整性改善: {log['improvement_summary']['missing_reduction']}个字段组
- 数据可用性: 显著提升

## 使用说明
1. 增强数据仅用于算法测试和学习
2. 不作为真实投资决策依据  
3. 建议结合公开信息进一步验证
"""
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report)


def main():
    """主函数 - 命令行使用"""
    import sys
    
    if len(sys.argv) < 3:
        print("使用方法：")
        print("python data_enhancer.py <input_file> <output_file>")
        print("支持 .csv 和 .json 格式")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    enhancer = EnterpriseDataEnhancer()
    
    print(f"开始增强数据: {input_file}")
    
    if input_file.endswith('.csv'):
        log = enhancer.enhance_csv_data(input_file, output_file)
    else:
        print("错误：目前只支持 .csv 格式")
        sys.exit(1)
    
    # 生成报告
    report_file = output_file.replace('.csv', '_enhancement_report.md')
    enhancer.generate_enhancement_report(log, report_file)
    
    print(f"数据增强完成!")
    print(f"输出文件: {output_file}")
    print(f"增强报告: {report_file}")
    print(f"增强了 {log['improvement_summary']['fields_enhanced']} 个字段")


if __name__ == "__main__":
    main()