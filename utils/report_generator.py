"""
Gerador de relatórios HTML para análise de dados financeiros.

Cria relatórios visuais e interativos com:
- Qualidade de dados
- Métricas financeiras
- Comparações
- Tabelas e estatísticas
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
import json


class HTMLReportGenerator:
    """Gera relatórios HTML a partir de análises"""
    
    def __init__(self):
        self.template = self._get_template()
    
    def generate_quality_report(
        self, 
        quality_data: Dict[str, Any],
        output_path: Optional[str] = None
    ) -> str:
        """
        Gera relatório HTML de qualidade de dados.
        
        Args:
            quality_data: Dados de analyze_data_quality()
            output_path: Caminho para salvar (None = retorna string)
            
        Returns:
            HTML como string
        """
        symbol = quality_data.get('symbol', 'N/A')
        period = quality_data.get('period', 'N/A')
        
        html_content = f"""
        <div class="report-section">
            <h2>📊 Relatório de Qualidade de Dados</h2>
            <h3>{symbol} - {period}</h3>
            
            <div class="metric-grid">
                {self._render_quality_metrics(quality_data)}
            </div>
            
            <div class="gaps-section">
                <h3>📉 Análise de Gaps</h3>
                {self._render_gaps_analysis(quality_data.get('gaps', {}))}
            </div>
            
            <div class="validation-section">
                <h3>✅ Issues de Validação</h3>
                {self._render_validation_issues(quality_data.get('validation_issues', {}))}
            </div>
            
            <div class="integrity-section">
                <h3>🔍 Integridade dos Dados</h3>
                {self._render_data_integrity(quality_data.get('data_integrity', {}))}
            </div>
        </div>
        """
        
        full_html = self._wrap_in_template(
            html_content, 
            f"Qualidade de Dados - {symbol}",
            quality_data
        )
        
        if output_path:
            Path(output_path).write_text(full_html, encoding='utf-8')
        
        return full_html
    
    def generate_financial_report(
        self,
        summary_stats: Dict[str, Any],
        output_path: Optional[str] = None
    ) -> str:
        """
        Gera relatório HTML de análise financeira.
        
        Args:
            summary_stats: Dados de get_summary_statistics()
            output_path: Caminho para salvar
            
        Returns:
            HTML como string
        """
        symbol = summary_stats.get('symbol', 'N/A')
        period = summary_stats.get('period', 'N/A')
        
        html_content = f"""
        <div class="report-section">
            <h2>💰 Relatório Financeiro</h2>
            <h3>{symbol} - {period}</h3>
            
            <div class="metric-grid">
                {self._render_price_metrics(summary_stats.get('price_metrics', {}))}
            </div>
            
            <div class="volatility-section">
                <h3>📊 Volatilidade</h3>
                {self._render_volatility_metrics(summary_stats.get('volatility', {}))}
            </div>
            
            <div class="drawdown-section">
                <h3>📉 Drawdown</h3>
                {self._render_drawdown_metrics(summary_stats.get('drawdown', {}))}
            </div>
            
            <div class="volume-section">
                <h3>📈 Volume</h3>
                {self._render_volume_metrics(summary_stats.get('volume_metrics', {}))}
            </div>
        </div>
        """
        
        full_html = self._wrap_in_template(
            html_content,
            f"Análise Financeira - {symbol}",
            summary_stats
        )
        
        if output_path:
            Path(output_path).write_text(full_html, encoding='utf-8')
        
        return full_html
    
    def generate_comparison_report(
        self,
        comparison_data: Dict[str, Any],
        correlation_matrix: Any,  # DataFrame
        symbols: List[str],
        output_path: Optional[str] = None
    ) -> str:
        """
        Gera relatório HTML de comparação entre símbolos.
        
        Args:
            comparison_data: Dados de compare_original_vs_corrected()
            correlation_matrix: DataFrame de correlação
            symbols: Lista de símbolos comparados
            output_path: Caminho para salvar
            
        Returns:
            HTML como string
        """
        html_content = f"""
        <div class="report-section">
            <h2>🔄 Relatório de Comparação</h2>
            <h3>Símbolos: {', '.join(symbols)}</h3>
            
            <div class="comparison-section">
                <h3>📊 Original vs Corrigido</h3>
                {self._render_original_vs_corrected(comparison_data)}
            </div>
            
            <div class="correlation-section">
                <h3>🔗 Matriz de Correlação</h3>
                {self._render_correlation_matrix(correlation_matrix)}
            </div>
        </div>
        """
        
        full_html = self._wrap_in_template(
            html_content,
            f"Comparação - {', '.join(symbols)}",
            {"comparison": comparison_data, "symbols": symbols}
        )
        
        if output_path:
            Path(output_path).write_text(full_html, encoding='utf-8')
        
        return full_html
    
    def generate_complete_report(
        self,
        quality_data: Dict[str, Any],
        financial_data: Dict[str, Any],
        volume_data: Dict[str, Any],
        comparison_data: Optional[Dict[str, Any]] = None,
        output_path: Optional[str] = None
    ) -> str:
        """
        Gera relatório completo com todas as seções.
        
        Returns:
            HTML como string
        """
        symbol = quality_data.get('symbol', financial_data.get('symbol', 'N/A'))
        period = quality_data.get('period', financial_data.get('period', 'N/A'))
        
        html_content = f"""
        <div class="report-header">
            <h1>📈 Relatório Completo de Análise</h1>
            <h2>{symbol} - {period}</h2>
            <p class="report-date">Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <div class="report-section">
            <h2>📊 Qualidade de Dados</h2>
            <div class="metric-grid">
                {self._render_quality_metrics(quality_data)}
            </div>
            <div class="gaps-section">
                <h3>📉 Análise de Gaps</h3>
                {self._render_gaps_analysis(quality_data.get('gaps', {}))}
            </div>
        </div>
        
        <div class="report-section">
            <h2>💰 Análise Financeira</h2>
            <div class="metric-grid">
                {self._render_price_metrics(financial_data.get('price_metrics', {}))}
            </div>
            <div class="volatility-section">
                <h3>📊 Volatilidade</h3>
                {self._render_volatility_metrics(financial_data.get('volatility', {}))}
            </div>
            <div class="drawdown-section">
                <h3>📉 Drawdown</h3>
                {self._render_drawdown_metrics(financial_data.get('drawdown', {}))}
            </div>
        </div>
        
        <div class="report-section">
            <h2>📈 Análise de Volume</h2>
            <div class="metric-grid">
                {self._render_volume_analysis(volume_data)}
            </div>
        </div>
        """
        
        if comparison_data:
            html_content += f"""
            <div class="report-section">
                <h2>🔄 Comparação Original vs Corrigido</h2>
                {self._render_original_vs_corrected(comparison_data)}
            </div>
            """
        
        full_html = self._wrap_in_template(
            html_content,
            f"Relatório Completo - {symbol}",
            {
                "quality": quality_data,
                "financial": financial_data,
                "volume": volume_data,
                "comparison": comparison_data
            }
        )
        
        if output_path:
            Path(output_path).write_text(full_html, encoding='utf-8')
        
        return full_html
    
    # ========================================================================
    # RENDER HELPERS
    # ========================================================================
    
    def _render_quality_metrics(self, data: Dict) -> str:
        """Renderiza métricas de qualidade"""
        completeness = data.get('completeness', {})
        date_range = data.get('date_range', {})
        
        return f"""
        <div class="metric-card">
            <h4>📅 Período</h4>
            <p class="metric-value">{date_range.get('start', 'N/A')} até {date_range.get('end', 'N/A')}</p>
            <p class="metric-label">{date_range.get('total_days', 0)} dias totais</p>
        </div>
        <div class="metric-card">
            <h4>📊 Completude Original</h4>
            <p class="metric-value">{completeness.get('completeness_original_pct', 0):.2f}%</p>
            <p class="metric-label">{completeness.get('original_records', 0)} registros</p>
        </div>
        <div class="metric-card success">
            <h4>✅ Completude Corrigida</h4>
            <p class="metric-value">{completeness.get('completeness_corrected_pct', 0):.2f}%</p>
            <p class="metric-label">{completeness.get('corrected_records', 0)} registros</p>
        </div>
        <div class="metric-card info">
            <h4>🔢 Dias de Mercado Esperados</h4>
            <p class="metric-value">{completeness.get('expected_trading_days', 0)}</p>
            <p class="metric-label">Estimativa (seg-sex)</p>
        </div>
        """
    
    def _render_gaps_analysis(self, gaps: Dict) -> str:
        """Renderiza análise de gaps"""
        gap_dist = gaps.get('gap_distribution', {})
        
        gap_rows = ""
        for gap_size, count in gap_dist.items():
            gap_rows += f"<tr><td>{gap_size}</td><td>{count}</td></tr>"
        
        return f"""
        <div class="metric-grid">
            <div class="metric-card warning">
                <h4>⚠️ Total de Gaps</h4>
                <p class="metric-value">{gaps.get('total_gaps', 0)}</p>
            </div>
            <div class="metric-card success">
                <h4>✅ Gaps Preenchidos</h4>
                <p class="metric-value">{gaps.get('gaps_filled_by_validation', 0)}</p>
            </div>
            <div class="metric-card">
                <h4>📏 Maior Gap</h4>
                <p class="metric-value">{gaps.get('largest_gap_days', 0)} dias</p>
            </div>
        </div>
        
        {f'''
        <table class="data-table">
            <thead>
                <tr><th>Tamanho do Gap</th><th>Quantidade</th></tr>
            </thead>
            <tbody>
                {gap_rows}
            </tbody>
        </table>
        ''' if gap_rows else '<p>Sem gaps detectados</p>'}
        """
    
    def _render_validation_issues(self, validation: Dict) -> str:
        """Renderiza issues de validação"""
        return f"""
        <div class="metric-grid">
            <div class="metric-card {'error' if validation.get('critical_issues', 0) > 0 else 'success'}">
                <h4>🔴 CRITICAL</h4>
                <p class="metric-value">{validation.get('critical_issues', 0)}</p>
            </div>
            <div class="metric-card {'warning' if validation.get('warnings', 0) > 0 else 'success'}">
                <h4>🟡 WARNING</h4>
                <p class="metric-value">{validation.get('warnings', 0)}</p>
            </div>
            <div class="metric-card info">
                <h4>🔵 INFO</h4>
                <p class="metric-value">{validation.get('info', 0)}</p>
            </div>
            <div class="metric-card">
                <h4>📋 Total</h4>
                <p class="metric-value">{validation.get('total_issues', 0)}</p>
            </div>
        </div>
        """
    
    def _render_data_integrity(self, integrity: Dict) -> str:
        """Renderiza integridade dos dados"""
        has_nulls = integrity.get('has_nulls', {})
        total_nulls = sum(has_nulls.values()) if has_nulls else 0
        
        null_rows = ""
        if has_nulls:
            for col, count in has_nulls.items():
                null_rows += f"<tr><td>{col}</td><td>{count}</td></tr>"
        
        return f"""
        <div class="metric-grid">
            <div class="metric-card {'error' if total_nulls > 0 else 'success'}">
                <h4>🔍 Valores Nulos</h4>
                <p class="metric-value">{total_nulls}</p>
            </div>
            <div class="metric-card {'error' if integrity.get('has_duplicates', 0) > 0 else 'success'}">
                <h4>🔄 Duplicatas</h4>
                <p class="metric-value">{integrity.get('has_duplicates', 0)}</p>
            </div>
            <div class="metric-card {'success' if integrity.get('is_sorted', False) else 'warning'}">
                <h4>📊 Ordenação</h4>
                <p class="metric-value">{'✅ Ordenado' if integrity.get('is_sorted', False) else '❌ Desordenado'}</p>
            </div>
        </div>
        
        {f'''
        <table class="data-table">
            <thead>
                <tr><th>Coluna</th><th>Nulos</th></tr>
            </thead>
            <tbody>
                {null_rows}
            </tbody>
        </table>
        ''' if null_rows else ''}
        """
    
    def _render_price_metrics(self, price: Dict) -> str:
        """Renderiza métricas de preço"""
        total_return = price.get('total_return_pct', 0)
        return_class = 'success' if total_return > 0 else 'error'
        
        return f"""
        <div class="metric-card">
            <h4>💵 Preço Inicial</h4>
            <p class="metric-value">${price.get('start_price', 0):.2f}</p>
        </div>
        <div class="metric-card">
            <h4>💵 Preço Final</h4>
            <p class="metric-value">${price.get('end_price', 0):.2f}</p>
        </div>
        <div class="metric-card {return_class}">
            <h4>📊 Retorno Total</h4>
            <p class="metric-value">{total_return:+.2f}%</p>
            <p class="metric-label">${price.get('price_change', 0):+.2f}</p>
        </div>
        <div class="metric-card success">
            <h4>📈 Maior Alta</h4>
            <p class="metric-value">${price.get('highest_price', 0):.2f}</p>
            <p class="metric-label">{price.get('highest_price_date', 'N/A')}</p>
        </div>
        <div class="metric-card error">
            <h4>📉 Maior Baixa</h4>
            <p class="metric-value">${price.get('lowest_price', 0):.2f}</p>
            <p class="metric-label">{price.get('lowest_price_date', 'N/A')}</p>
        </div>
        """
    
    def _render_volatility_metrics(self, vol: Dict) -> str:
        """Renderiza métricas de volatilidade"""
        return f"""
        <div class="metric-grid">
            <div class="metric-card">
                <h4>📊 Volatilidade Anualizada</h4>
                <p class="metric-value">{vol.get('annualized_volatility_pct', 0):.2f}%</p>
            </div>
            <div class="metric-card">
                <h4>📉 Vol. Rolling Atual</h4>
                <p class="metric-value">{vol.get('current_rolling_vol', 0):.4f}</p>
            </div>
            <div class="metric-card">
                <h4>⬆️ Vol. Máxima</h4>
                <p class="metric-value">{vol.get('max_rolling_vol', 0):.4f}</p>
            </div>
            <div class="metric-card">
                <h4>⬇️ Vol. Mínima</h4>
                <p class="metric-value">{vol.get('min_rolling_vol', 0):.4f}</p>
            </div>
        </div>
        """
    
    def _render_drawdown_metrics(self, dd: Dict) -> str:
        """Renderiza métricas de drawdown"""
        max_dd = dd.get('maximum_drawdown_pct', 0)
        current_dd = dd.get('current_drawdown_pct', 0)
        
        return f"""
        <div class="metric-grid">
            <div class="metric-card error">
                <h4>📉 Maximum Drawdown</h4>
                <p class="metric-value">{max_dd:.2f}%</p>
                <p class="metric-label">{dd.get('max_drawdown_date', 'N/A')}</p>
            </div>
            <div class="metric-card warning">
                <h4>📊 Drawdown Atual</h4>
                <p class="metric-value">{current_dd:.2f}%</p>
            </div>
            <div class="metric-card">
                <h4>📅 Dias em Drawdown</h4>
                <p class="metric-value">{dd.get('days_in_drawdown', 0)}</p>
            </div>
        </div>
        """
    
    def _render_volume_metrics(self, vol: Dict) -> str:
        """Renderiza métricas de volume"""
        return f"""
        <div class="metric-card">
            <h4>📊 Volume Médio</h4>
            <p class="metric-value">{vol.get('average_volume', 0):,}</p>
        </div>
        <div class="metric-card">
            <h4>📈 Volume Máximo</h4>
            <p class="metric-value">{vol.get('max_volume', 0):,}</p>
            <p class="metric-label">{vol.get('max_volume_date', 'N/A')}</p>
        </div>
        <div class="metric-card">
            <h4>🔢 Volume Total</h4>
            <p class="metric-value">{vol.get('total_volume', 0):,}</p>
        </div>
        """
    
    def _render_volume_analysis(self, vol: Dict) -> str:
        """Renderiza análise completa de volume"""
        return f"""
        <div class="metric-card">
            <h4>📊 Volume Médio</h4>
            <p class="metric-value">{vol.get('average_volume', 0):,}</p>
        </div>
        <div class="metric-card">
            <h4>📏 Desvio Padrão</h4>
            <p class="metric-value">{vol.get('std_volume', 0):,}</p>
        </div>
        <div class="metric-card">
            <h4>📊 Volume Mediano</h4>
            <p class="metric-value">{vol.get('median_volume', 0):,}</p>
        </div>
        <div class="metric-card warning">
            <h4>⚠️ Dias com Volume Anômalo</h4>
            <p class="metric-value">{vol.get('anomalous_volume_days', 0)}</p>
        </div>
        <div class="metric-card error">
            <h4>❌ Dias com Volume Zero</h4>
            <p class="metric-value">{vol.get('zero_volume_days', 0)}</p>
        </div>
        <div class="metric-card">
            <h4>🎯 Top 10 Dias (% Volume)</h4>
            <p class="metric-value">{vol.get('volume_concentration', {}).get('top_10_days_volume_pct', 0):.2f}%</p>
        </div>
        """
    
    def _render_original_vs_corrected(self, comp: Dict) -> str:
        """Renderiza comparação original vs corrigido"""
        records = comp.get('records', {})
        price_orig = comp.get('price_statistics', {}).get('original', {})
        price_corr = comp.get('price_statistics', {}).get('corrected', {})
        
        return f"""
        <h4>📊 Registros</h4>
        <div class="metric-grid">
            <div class="metric-card">
                <h4>Original</h4>
                <p class="metric-value">{records.get('original', 0)}</p>
            </div>
            <div class="metric-card success">
                <h4>Corrigido</h4>
                <p class="metric-value">{records.get('corrected', 0)}</p>
            </div>
            <div class="metric-card info">
                <h4>Adicionados</h4>
                <p class="metric-value">+{records.get('added', 0)}</p>
            </div>
        </div>
        
        <h4>💰 Estatísticas de Preço</h4>
        <table class="data-table">
            <thead>
                <tr><th>Métrica</th><th>Original</th><th>Corrigido</th></tr>
            </thead>
            <tbody>
                <tr>
                    <td>Média Close</td>
                    <td>${price_orig.get('mean_close', 0):.2f}</td>
                    <td>${price_corr.get('mean_close', 0):.2f}</td>
                </tr>
                <tr>
                    <td>Desvio Padrão</td>
                    <td>${price_orig.get('std_close', 0):.2f}</td>
                    <td>${price_corr.get('std_close', 0):.2f}</td>
                </tr>
                <tr>
                    <td>Mínimo</td>
                    <td>${price_orig.get('min_close', 0):.2f}</td>
                    <td>${price_corr.get('min_close', 0):.2f}</td>
                </tr>
                <tr>
                    <td>Máximo</td>
                    <td>${price_orig.get('max_close', 0):.2f}</td>
                    <td>${price_corr.get('max_close', 0):.2f}</td>
                </tr>
            </tbody>
        </table>
        """
    
    def _render_correlation_matrix(self, corr_matrix: Any) -> str:
        """Renderiza matriz de correlação"""
        if corr_matrix is None or corr_matrix.empty:
            return "<p>Sem dados de correlação</p>"
        
        # Gerar tabela HTML
        rows = ""
        for idx in corr_matrix.index:
            row = f"<tr><td><strong>{idx}</strong></td>"
            for col in corr_matrix.columns:
                val = corr_matrix.loc[idx, col]
                color_class = 'success' if val > 0.7 else ('warning' if val > 0.3 else 'error')
                row += f'<td class="{color_class}">{val:.3f}</td>'
            row += "</tr>"
            rows += row
        
        header = "<tr><th></th>" + "".join([f"<th>{col}</th>" for col in corr_matrix.columns]) + "</tr>"
        
        return f"""
        <table class="data-table">
            <thead>{header}</thead>
            <tbody>{rows}</tbody>
        </table>
        <p class="metric-label">Correlação: <span class="success">Alta (>0.7)</span> | <span class="warning">Média (0.3-0.7)</span> | <span class="error">Baixa (<0.3)</span></p>
        """
    
    # ========================================================================
    # TEMPLATE
    # ========================================================================
    
    def _get_template(self) -> str:
        """Retorna template HTML base"""
        return """
        <!DOCTYPE html>
        <html lang="pt-BR">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{title}</title>
            <style>
                * {{
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }}
                
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: #333;
                    padding: 20px;
                    line-height: 1.6;
                }}
                
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background: white;
                    border-radius: 15px;
                    padding: 40px;
                    box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                }}
                
                .report-header {{
                    text-align: center;
                    border-bottom: 3px solid #667eea;
                    padding-bottom: 20px;
                    margin-bottom: 30px;
                }}
                
                .report-header h1 {{
                    color: #667eea;
                    font-size: 2.5em;
                    margin-bottom: 10px;
                }}
                
                .report-header h2 {{
                    color: #764ba2;
                    font-size: 1.8em;
                }}
                
                .report-date {{
                    color: #666;
                    font-size: 0.9em;
                    margin-top: 10px;
                }}
                
                .report-section {{
                    margin-bottom: 40px;
                    padding: 20px;
                    background: #f8f9fa;
                    border-radius: 10px;
                }}
                
                .report-section h2 {{
                    color: #667eea;
                    font-size: 1.8em;
                    margin-bottom: 20px;
                    border-left: 5px solid #667eea;
                    padding-left: 15px;
                }}
                
                .report-section h3 {{
                    color: #764ba2;
                    font-size: 1.3em;
                    margin: 20px 0 15px 0;
                }}
                
                .metric-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 20px;
                    margin: 20px 0;
                }}
                
                .metric-card {{
                    background: white;
                    padding: 20px;
                    border-radius: 10px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                    border-left: 5px solid #667eea;
                    transition: transform 0.2s;
                }}
                
                .metric-card:hover {{
                    transform: translateY(-5px);
                    box-shadow: 0 5px 20px rgba(0,0,0,0.2);
                }}
                
                .metric-card.success {{
                    border-left-color: #28a745;
                }}
                
                .metric-card.warning {{
                    border-left-color: #ffc107;
                }}
                
                .metric-card.error {{
                    border-left-color: #dc3545;
                }}
                
                .metric-card.info {{
                    border-left-color: #17a2b8;
                }}
                
                .metric-card h4 {{
                    color: #666;
                    font-size: 0.9em;
                    text-transform: uppercase;
                    margin-bottom: 10px;
                }}
                
                .metric-value {{
                    font-size: 2em;
                    font-weight: bold;
                    color: #333;
                    margin: 10px 0;
                }}
                
                .metric-label {{
                    color: #999;
                    font-size: 0.85em;
                }}
                
                .data-table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin: 20px 0;
                    background: white;
                    border-radius: 10px;
                    overflow: hidden;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                }}
                
                .data-table thead {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                }}
                
                .data-table th,
                .data-table td {{
                    padding: 15px;
                    text-align: left;
                }}
                
                .data-table tbody tr:nth-child(even) {{
                    background: #f8f9fa;
                }}
                
                .data-table tbody tr:hover {{
                    background: #e9ecef;
                }}
                
                .data-table td.success {{
                    background: #d4edda;
                    color: #155724;
                    font-weight: bold;
                }}
                
                .data-table td.warning {{
                    background: #fff3cd;
                    color: #856404;
                    font-weight: bold;
                }}
                
                .data-table td.error {{
                    background: #f8d7da;
                    color: #721c24;
                    font-weight: bold;
                }}
                
                .json-data {{
                    background: #2d2d2d;
                    color: #f8f8f2;
                    padding: 20px;
                    border-radius: 10px;
                    overflow-x: auto;
                    font-family: 'Courier New', monospace;
                    font-size: 0.9em;
                    margin: 20px 0;
                }}
                
                @media print {{
                    body {{
                        background: white;
                    }}
                    .container {{
                        box-shadow: none;
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                {content}
                
                <div class="report-section">
                    <h3>📄 Dados Brutos (JSON)</h3>
                    <pre class="json-data">{json_data}</pre>
                </div>
            </div>
        </body>
        </html>
        """
    
    def _wrap_in_template(self, content: str, title: str, data: Dict) -> str:
        """Envolve conteúdo no template HTML"""
        json_data = json.dumps(data, indent=2, ensure_ascii=False, default=str)
        
        return self.template.format(
            title=title,
            content=content,
            json_data=json_data
        )
