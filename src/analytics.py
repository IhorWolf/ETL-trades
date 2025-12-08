class TopClientsAnalytics:
    def __init__(self, df):
        self.df = df
    
    def get_top_clients_by_volume(self, top_n=3):
        """Get top N bronze clients by trading volume"""
        bronze_clients = self.df[self.df['client_type'] == 'bronze']
        
        top_clients = bronze_clients.groupby('user_id').agg({
            'total_volume': 'sum'
        }).reset_index()
        
        top_clients = top_clients.nlargest(top_n, 'total_volume')
        top_clients.columns = ['user_id', 'total_volume']
        
        return top_clients
    
    def get_top_clients_by_pnl(self, top_n=3):
        """Get top N bronze clients by PnL"""
        bronze_clients = self.df[self.df['client_type'] == 'bronze']
        
        top_clients = bronze_clients.groupby('user_id').agg({
            'closed_pnl': 'sum'
        }).reset_index()
        
        top_clients = top_clients.nlargest(top_n, 'closed_pnl')
        top_clients.columns = ['user_id', 'total_pnl']
        
        return top_clients
