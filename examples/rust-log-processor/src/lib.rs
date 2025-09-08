use wasm_bindgen::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: String,
    pub container_id: String,
    pub log_line: String,
    pub level: String,
}

#[derive(Debug, Serialize)]
pub struct LogAnalysis {
    pub total_logs: usize,
    pub error_count: usize,
    pub warn_count: usize,
    pub info_count: usize,
    pub debug_count: usize,
    pub suspicious_patterns: Vec<String>,
}

#[wasm_bindgen]
pub fn process_logs(log_data: &str) -> Result<String, JsValue> {
    // Parse the JSON log data
    let logs: Vec<LogEntry> = serde_json::from_str(log_data)
        .map_err(|e| JsValue::from_str(&format!("Failed to parse logs: {}", e)))?;
    
    // Analyze the logs
    let analysis = analyze_logs(&logs);
    
    // Return the analysis as JSON
    serde_json::to_string(&analysis)
        .map_err(|e| JsValue::from_str(&format!("Failed to serialize analysis: {}", e)))
}

fn analyze_logs(logs: &[LogEntry]) -> LogAnalysis {
    let mut error_count = 0;
    let mut warn_count = 0;
    let mut info_count = 0;
    let mut debug_count = 0;
    let mut suspicious_patterns = Vec::new();
    
    for log in logs {
        match log.level.as_str() {
            "ERROR" => error_count += 1,
            "WARN" => warn_count += 1,
            "INFO" => info_count += 1,
            "DEBUG" => debug_count += 1,
            _ => {}
        }
        
        // Check for suspicious patterns
        let lower_log = log.log_line.to_lowercase();
        if lower_log.contains("password") || lower_log.contains("secret") || lower_log.contains("key") {
            suspicious_patterns.push("Potential credential leak detected".to_string());
        }
        
        if lower_log.contains("exception") || lower_log.contains("stack trace") {
            suspicious_patterns.push("Exception or stack trace detected".to_string());
        }
        
        if lower_log.contains("timeout") || lower_log.contains("deadlock") {
            suspicious_patterns.push("Performance issue detected".to_string());
        }
    }
    
    LogAnalysis {
        total_logs: logs.len(),
        error_count,
        warn_count,
        info_count,
        debug_count,
        suspicious_patterns,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_analyze_logs() {
        let logs = vec![
            LogEntry {
                timestamp: "2024-01-01T00:00:00Z".to_string(),
                container_id: "test".to_string(),
                log_line: "ERROR: Something went wrong".to_string(),
                level: "ERROR".to_string(),
            },
            LogEntry {
                timestamp: "2024-01-01T00:00:01Z".to_string(),
                container_id: "test".to_string(),
                log_line: "INFO: Application started".to_string(),
                level: "INFO".to_string(),
            },
        ];
        
        let analysis = analyze_logs(&logs);
        assert_eq!(analysis.total_logs, 2);
        assert_eq!(analysis.error_count, 1);
        assert_eq!(analysis.info_count, 1);
    }
} 