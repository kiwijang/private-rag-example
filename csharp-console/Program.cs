using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using System.Text.Json; // For parsing LLM response
using System.IO;        // Add this for File/Directory operations
using PaddleOCRSharp;   // Add this for PaddleOCR

namespace LocalRagConsole
{
    class Program
    {
        // Database connection string
        // Note: Ensure your Postgres container/service is accessible via localhost:5432 with these credentials
        private const string ConnectionString = "Host=localhost;Username=postgres;Password=1234;Database=postgres;Port=5432";

        // Host for Ollama service (as seen by the Postgres server if running in Docker, or local machine)
        // If Postgres is in Docker and Ollama is in Docker on same network 'local-rag': use 'http://ollama:11434'
        // If running locally: 'http://localhost:11434'
        private const string OllamaHost = "http://ollama:11434";

        static async Task Main(string[] args)
        {
            Console.WriteLine("🚀 Starting Local RAG Console App (C#)...");

            try
            {
                await RunRagApp();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ Error: {ex.Message}");
                Console.WriteLine("Stack Trace: " + ex.StackTrace);
            }
        }

        static async Task RunRagApp()
        {
            await using var dataSource = NpgsqlDataSource.Create(ConnectionString);

            // 1. Install Extension
            Console.WriteLine("\n[1/6] Installing pgai extension...");
            // Increase timeout for extension installation
            await using (var cmd = dataSource.CreateCommand("CREATE EXTENSION IF NOT EXISTS \"ai\" CASCADE;"))
            {
                cmd.CommandTimeout = 300; // 5 minutes
                await cmd.ExecuteNonQueryAsync();
            }

            // 2. Check function existence
            Console.WriteLine("\n[2/6] Checking ollama_embed function...");
            await using (var cmd = dataSource.CreateCommand("SELECT count(*) FROM pg_proc WHERE proname = 'ollama_embed';"))
            {
                var count = (long)(await cmd.ExecuteScalarAsync() ?? 0);
                if (count > 0) Console.WriteLine("   ✅ Function 'ollama_embed' exists.");
                else
                {
                    Console.WriteLine("   ❌ Function 'ollama_embed' does NOT exist.");
                    return;
                }
            }

            // 3. Create Table
            Console.WriteLine("\n[3/6] Creating table 'documents'...");
            var createTableSql = @"
                CREATE TABLE IF NOT EXISTS documents (
                    id SERIAL PRIMARY KEY,
                    title TEXT,
                    content TEXT,
                    embedding VECTOR(768)
                );";
            await using (var cmd = dataSource.CreateCommand(createTableSql))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            // 4. Insert Data
            Console.WriteLine("\n[4/6] Inserting dummy data (Generating Embeddings via Ollama)...");

            // Clean table first to avoid duplicates
            await using (var truncCmd = dataSource.CreateCommand("TRUNCATE TABLE documents;"))
            {
                await truncCmd.ExecuteNonQueryAsync();
            }

            var documentsToInsert = new List<dynamic>();

            // 檢查是否有 images 資料夾
            string imagesPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "images");

            if (Directory.Exists(imagesPath))
            {
                Console.WriteLine($"   📂 Found 'images' folder: {imagesPath}");
                Console.WriteLine("   🔍 Initializing PaddleOCR engine...");

                // 建立 Config 並嘗試對應你的資料夾模型名稱 (yt_PP-OCRv5)
                OCRModelConfig config = new OCRModelConfig();
                string inferencePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "inference");

                // 自動偵測並設定模型路徑 (若存在)
                string detPath = Path.Combine(inferencePath, "yt_PP-OCRv5_mobile_det_infer");
                string clsPath = Path.Combine(inferencePath, "yt_PP-OCRv5_mobile_cls_infer");
                string recPath = Path.Combine(inferencePath, "yt_PP-OCRv5_mobile_rec_infer");

                if (Directory.Exists(detPath)) config.det_infer = detPath;
                if (Directory.Exists(clsPath)) config.cls_infer = clsPath;
                if (Directory.Exists(recPath)) config.rec_infer = recPath;
                // 嘗試尋找字典檔 (通常在 inference 根目錄或 rec 資料夾內)
                // config.keys = Path.Combine(inferencePath, "ppocr_keys_v1.txt"); 

                OCRParameter parameter = new OCRParameter();

                // PaddleOCREngine does not implement IDisposable
                PaddleOCREngine engine = new PaddleOCREngine(config, parameter);

                string[] imageFiles = Directory.GetFiles(imagesPath, "*.*");
                foreach (var imgFile in imageFiles)
                {
                    string fileName = Path.GetFileNameWithoutExtension(imgFile);
                    string extension = Path.GetExtension(imgFile).ToLower();
                    if (extension != ".jpg" && extension != ".png" && extension != ".bmp") continue;

                    Console.WriteLine($"   Processing Image: {fileName}");

                    try
                    {
                        // 1. OCR 識別
                        OCRResult ocrResult = engine.DetectText(imgFile);
                        string rawText = ocrResult != null ? ocrResult.Text : "";

                        if (string.IsNullOrWhiteSpace(rawText))
                        {
                            Console.WriteLine($"      ⚠️ No text detected in {fileName}");
                            continue;
                        }

                        // 2. 使用 Qwen2.5 進行摘要 (透過資料庫呼叫 Ollama)
                        Console.WriteLine("      📝 Generating Summary with qwen2.5:32b...");
                        string summaryPrompt = $"請將以下 OCR 識別出的文字整理成一段通順的摘要，保留關鍵資訊：\n\n{rawText}";

                        // 利用 Npgsql 執行 SQL 呼叫 ai.ollama_generate
                        var summarySql = "SELECT ai.ollama_generate('qwen2.5:32b', @prompt, host => @ollama_host);";
                        string summarizedContent = rawText; // 預設為原文

                        await using (var sumCmd = dataSource.CreateCommand(summarySql))
                        {
                            sumCmd.Parameters.AddWithValue("prompt", summaryPrompt);
                            sumCmd.Parameters.AddWithValue("ollama_host", OllamaHost);

                            var resultJson = await sumCmd.ExecuteScalarAsync() as string;
                            if (!string.IsNullOrEmpty(resultJson))
                            {
                                try
                                {
                                    using var jsonDoc = JsonDocument.Parse(resultJson);
                                    if (jsonDoc.RootElement.TryGetProperty("response", out var resp))
                                    {
                                        summarizedContent = resp.GetString();
                                    }
                                }
                                catch { /* Ignore JSON parse error */ }
                            }
                        }

                        documentsToInsert.Add(new { Title = fileName, Content = summarizedContent });
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"      ❌ OCR Error on {fileName}: {ex.Message}");
                    }
                }
            }
            else
            {
                // Fallback to dummy data
                Console.WriteLine("   ⚠️ 'images' folder not found. Using hardcoded dummy data.");
                documentsToInsert.AddRange(new[]
                {
                    new { Title = "Seoul Tower", Content = "Seoul Tower is a communication and observation tower located on Namsan Mountain in central Seoul, South Korea." },
                    new { Title = "Gwanghwamun Gate", Content = "Gwanghwamun is the main and largest gate of Gyeongbokgung Palace, in Jongno-gu, Seoul, South Korea." },
                    new { Title = "Bukchon Hanok Village", Content = "Bukchon Hanok Village is a Korean traditional village in Seoul with a long history." },
                    new { Title = "Myeong-dong Shopping Street", Content = "Myeong-dong is one of the primary shopping districts in Seoul, South Korea." },
                    new { Title = "Dongdaemun Design Plaza", Content = "The Dongdaemun Design Plaza is a major urban development landmark in Seoul, South Korea." }
                });
            }


            foreach (var doc in documentsToInsert)
            {
                // Note: Using 'ai.ollama_embed' as confirmed in notebook fixes
                var insertSql = @"
                    INSERT INTO documents (title, content, embedding)
                    VALUES (
                        @title, 
                        @content, 
                        ai.ollama_embed('nomic-embed-text', @text_to_embed, host => @ollama_host)
                    );";

                await using var cmd = dataSource.CreateCommand(insertSql);
                cmd.CommandTimeout = 300; // 5 minutes in case Ollama pull/load model takes time
                cmd.Parameters.AddWithValue("title", doc.Title);
                cmd.Parameters.AddWithValue("content", doc.Content);
                cmd.Parameters.AddWithValue("text_to_embed", $"{doc.Title} - {doc.Content}");
                cmd.Parameters.AddWithValue("ollama_host", OllamaHost);

                await cmd.ExecuteNonQueryAsync();
                Console.WriteLine($"   Inserted: {doc.Title}");
            }

            // 5. Retrieval
            Console.WriteLine("\n[5/6] RAG Retrieval...");
            string query = "基金的種類有哪些?";
            Console.WriteLine($"   Query: \"{query}\"");

            // We perform calculation in SQL to avoid manual vector handling in C#
            // 1. Embed Query
            // 2. Calculate Distance
            // 3. Select Top 3
            var searchSql = @"
                WITH query_vec AS (
                    SELECT ai.ollama_embed('nomic-embed-text', @query, host => @ollama_host) AS embedding
                )
                SELECT title, content 
                FROM documents, query_vec
                ORDER BY documents.embedding <=> query_vec.embedding
                LIMIT 3;";

            var contextParts = new List<string>();

            await using (var cmd = dataSource.CreateCommand(searchSql))
            {
                cmd.Parameters.AddWithValue("query", query);
                cmd.Parameters.AddWithValue("ollama_host", OllamaHost);

                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    string title = reader.GetString(0);
                    string content = reader.GetString(1);
                    contextParts.Add($"Title: {title}\nContent: {content}");
                }
            }

            string context = string.Join("\n\n", contextParts);
            Console.WriteLine($"   ✅ Context Retrieved ({contextParts.Count} docs)");

            // 6. Generation
            Console.WriteLine("\n[6/6] Generating LLM Response...");

            string fullPrompt = $"Query: {query}\nContext: {context}";

            // Call ai.ollama_generate
            var generateSql = "SELECT ai.ollama_generate('llama3.2', @prompt, host => @ollama_host);";

            await using (var cmd = dataSource.CreateCommand(generateSql))
            {
                cmd.CommandTimeout = 600; // 10 minutes (LLM generation can be slow)
                cmd.Parameters.AddWithValue("prompt", fullPrompt);
                cmd.Parameters.AddWithValue("ollama_host", OllamaHost);

                // Npgsql default mapping for JSONB is usually string (or char[]). 
                // We cast to string in C# explicitly.
                var resultObj = await cmd.ExecuteScalarAsync();
                if (resultObj is string resultJson)
                {
                    try
                    {
                        using var doc = JsonDocument.Parse(resultJson);
                        if (doc.RootElement.TryGetProperty("response", out var responseProp))
                        {
                            Console.WriteLine("\n🤖 LLM Response:");
                            Console.WriteLine("--------------------------------------------------");
                            Console.WriteLine(responseProp.GetString());
                            Console.WriteLine("--------------------------------------------------");
                        }
                        else
                        {
                            Console.WriteLine($"\nRaw Response (No 'response' field): {resultJson}");
                        }
                    }
                    catch (JsonException)
                    {
                        Console.WriteLine($"\nRaw Response (Not JSON): {resultJson}");
                    }
                }
            }
        }
    }
}
