// rootara_reader.go
// 这个脚本用于转换不同的厂商提供的结果文件，并储存到数据库中
//
// 已支持：
// - 23andMeV5
// - Wegene
// - AncestryDNA
// 确实分析效率高很多很多

package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// 定义数据结构
type RootaraRecord struct {
	Chrom     string
	Start     string
	Ref       string
	Alt       string
	Gene      string
	RSID      string
	GnomAD_AF string
	CLNSIG    string
	CLNDN     string
	Genotype  string
	Check     string
}

// 读取rootara核心库
func readRootaraCore(filePath string) (map[string]map[string]RootaraRecord, error) {
	// 打开gzip压缩文件
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("无法打开文件: %v", err)
	}
	defer file.Close()

	// 创建gzip读取器
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("无法创建gzip读取器: %v", err)
	}
	defer gzReader.Close()

	// 创建CSV读取器
	reader := csv.NewReader(gzReader)
	reader.Comma = '\t'

	// 读取标题行
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("无法读取标题行: %v", err)
	}

	// 创建列名到索引的映射
	colIndex := make(map[string]int)
	for i, col := range header {
		colIndex[col] = i
	}

	// 检查必要的列是否存在
	requiredCols := []string{"Chrom", "Start", "Ref", "Alt", "Gene", "RSID", "gnomAD_AF", "CLNSIG", "CLNDN"}
	for _, col := range requiredCols {
		if _, exists := colIndex[col]; !exists {
			return nil, fmt.Errorf("缺少必要的列: %s", col)
		}
	}

	// 创建数据结构存储记录，使用染色体和位置作为键
	records := make(map[string]map[string]RootaraRecord)

	// 读取并处理每一行
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("读取行时出错: %v", err)
		}

		// 获取染色体和位置
		chrom := row[colIndex["Chrom"]]
		start := row[colIndex["Start"]]
		ref := row[colIndex["Ref"]]
		alt := row[colIndex["Alt"]]

		// 处理染色体名称
		chrom = strings.ReplaceAll(chrom, "chrM", "MT")
		chrom = strings.ReplaceAll(chrom, "chr", "")

		// 调整插入缺失
		if len(ref) > len(alt) {
			ref = "I"
			alt = "D"
		} else if len(ref) < len(alt) {
			ref = "D"
			alt = "I"
		} else if len(ref) == len(alt) {
			if len(ref) == 1 {
				if alt == "-" {
					ref = "I"
					alt = "D"
				} else if ref == "-" {
					ref = "D"
					alt = "I"
				}
			} else {
				// MNV || 难以处理，这种位点可以过滤掉
				// 与Python版本保持一致，跳过这些记录
				continue
			}
		}

		// 创建记录
		record := RootaraRecord{
			Chrom:     chrom,
			Start:     start,
			Ref:       ref,
			Alt:       alt,
			Gene:      row[colIndex["Gene"]],
			RSID:      row[colIndex["RSID"]],
			GnomAD_AF: row[colIndex["gnomAD_AF"]],
			CLNSIG:    row[colIndex["CLNSIG"]],
			CLNDN:     row[colIndex["CLNDN"]],
		}

		// 将记录添加到映射中
		if _, exists := records[chrom]; !exists {
			records[chrom] = make(map[string]RootaraRecord)
		}
		records[chrom][start] = record
	}

	return records, nil
}

// 合并数据框架
func mergeDataFrames(inputRecords [][]string, rootaraRecords map[string]map[string]RootaraRecord, colIndex map[string]int) []RootaraRecord {
	var mergedRecords []RootaraRecord

	for _, row := range inputRecords {
		chrom := row[colIndex["Chrom"]]
		start := row[colIndex["Start"]]
		genotype := row[colIndex["Genotype"]]

		// 检查染色体和位置是否存在于rootara记录中
		if chromRecords, exists := rootaraRecords[chrom]; exists {
			if record, exists := chromRecords[start]; exists {
				// 检查基因型是否匹配
				ref := record.Ref
				alt := record.Alt

				// 创建所有可能的基因型组合
				allTypeList := []string{
					ref + ref,
					ref + alt,
					alt + alt,
					alt + ref,
				}

				// 去重
				uniqueTypes := make(map[string]bool)
				for _, t := range allTypeList {
					uniqueTypes[t] = true
				}

				// 检查基因型是否在可能的组合中
				matched := false
				for t := range uniqueTypes {
					if genotype == t {
						matched = true
						break
					}
				}

				if matched && genotype != "--" {
					// 基因型转换
					check := "NA"
					refCount := strings.Count(genotype, ref)
					if refCount == 2 {
						check = "WT"
					} else if refCount == 1 {
						check = "HET"
					} else if refCount == 0 {
						check = "HOM"
					}

					if check != "NA" {
						record.Genotype = genotype
						record.Check = check
						mergedRecords = append(mergedRecords, record)
					}
				}
			}
		}
	}

	return mergedRecords
}

// 读取通用格式结果
func readUniResult(filePath string, rootaraRecords map[string]map[string]RootaraRecord) ([]RootaraRecord, error) {
	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("无法打开文件: %v", err)
	}
	defer file.Close()

	// 创建扫描器
	scanner := bufio.NewScanner(file)
	
	// 定义列名
	columns := []string{"RSID", "Chrom", "Start", "Genotype"}
	colIndex := make(map[string]int)
	for i, col := range columns {
		colIndex[col] = i
	}

	// 读取数据
	var records [][]string
	for scanner.Scan() {
		line := scanner.Text()
		
		// 跳过注释行和空行
		if strings.HasPrefix(line, "#") || len(strings.TrimSpace(line)) == 0 {
			continue
		}
		
		fields := strings.Split(line, "\t")
		if len(fields) >= len(columns) {
			records = append(records, fields)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取文件时出错: %v", err)
	}

	beforeCount := len(records)
	mergedRecords := mergeDataFrames(records, rootaraRecords, colIndex)
	afterCount := len(mergedRecords)

	transRate := float64(afterCount) / float64(beforeCount) * 100
	fmt.Printf("转换率：%.2f%%\n", transRate)
	fmt.Printf("转换前数量：%d\n", beforeCount)
	fmt.Printf("转换后数量：%d\n", afterCount)

	return mergedRecords, nil
}

// 读取23andme结果
func read23andmeResult(filePath string, rootaraRecords map[string]map[string]RootaraRecord) ([]RootaraRecord, error) {
	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("无法打开文件: %v", err)
	}
	defer file.Close()

	// 创建扫描器
	scanner := bufio.NewScanner(file)
	
	// 定义列名
	columns := []string{"RSID", "Chrom", "Start", "Genotype"}
	colIndex := make(map[string]int)
	for i, col := range columns {
		colIndex[col] = i
	}

	// 读取数据
	var records [][]string
	for scanner.Scan() {
		line := scanner.Text()
		
		// 跳过注释行和空行
		if strings.HasPrefix(line, "#") || len(strings.TrimSpace(line)) == 0 {
			continue
		}
		
		fields := strings.Split(line, "\t")
		if len(fields) >= len(columns) {
			// 处理X、Y、MT的单个碱基
			if len(fields[3]) == 1 {
				fields[3] = fields[3] + fields[3]
			}
			records = append(records, fields)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取文件时出错: %v", err)
	}

	beforeCount := len(records)
	mergedRecords := mergeDataFrames(records, rootaraRecords, colIndex)
	afterCount := len(mergedRecords)

	transRate := float64(afterCount) / float64(beforeCount) * 100
	fmt.Printf("转换率：%.2f%%\n", transRate)
	fmt.Printf("转换前数量：%d\n", beforeCount)
	fmt.Printf("转换后数量：%d\n", afterCount)

	return mergedRecords, nil
}

// 读取AncestryDNA结果
func readAncestryResult(filePath string, rootaraRecords map[string]map[string]RootaraRecord) ([]RootaraRecord, error) {
	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("无法打开文件: %v", err)
	}
	defer file.Close()

	// 创建CSV读取器
	reader := csv.NewReader(file)
	reader.Comma = '\t'
	reader.Comment = '#'

	// 读取标题行
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("无法读取标题行: %v", err)
	}

	// 创建列名到索引的映射
	colIndex := make(map[string]int)
	for i, col := range header {
		colIndex[col] = i
	}

	// 检查必要的列是否存在
	requiredCols := []string{"rsid", "chromosome", "position", "allele1", "allele2"}
	for _, col := range requiredCols {
		if _, exists := colIndex[col]; !exists {
			return nil, fmt.Errorf("缺少必要的列: %s", col)
		}
	}

	// 创建新的列名映射 - 修改这里，不再使用-1作为Genotype的索引
	newColIndex := map[string]int{
		"RSID":     colIndex["rsid"],
		"Chrom":    colIndex["chromosome"],
		"Start":    colIndex["position"],
		"Genotype": 3, // 设置为新记录中的实际索引位置
	}

	// 读取数据
	var records [][]string
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("读取行时出错: %v", err)
		}

		// 处理染色体编号
		chrom := row[colIndex["chromosome"]]
		
		// 与Python版本保持一致，直接跳过PAR区
		if chrom == "25" {
			continue
		}
		
		// 使用与Python版本相同的替换逻辑
		chrom = strings.ReplaceAll(chrom, "23", "X")
		chrom = strings.ReplaceAll(chrom, "24", "Y")
		chrom = strings.ReplaceAll(chrom, "26", "MT")

		// 创建基因型
		genotype := row[colIndex["allele1"]] + row[colIndex["allele2"]]

		// 创建新记录
		newRow := []string{
			row[colIndex["rsid"]],
			chrom,
			row[colIndex["position"]],
			genotype,
		}
		records = append(records, newRow)
	}

	beforeCount := len(records)
	mergedRecords := mergeDataFrames(records, rootaraRecords, newColIndex)
	afterCount := len(mergedRecords)

	transRate := float64(afterCount) / float64(beforeCount) * 100
	fmt.Printf("转换率：%.2f%%\n", transRate)
	fmt.Printf("转换前数量：%d\n", beforeCount)
	fmt.Printf("转换后数量：%d\n", afterCount)

	return mergedRecords, nil
}

// 创建CSV文件
func csvCreate(inputPath, outputPath, method, rootaraCorePath string) error {
	// 读取rootara核心库
	rootaraRecords, err := readRootaraCore(rootaraCorePath)
	if err != nil {
		return fmt.Errorf("读取Rootara核心库失败: %v", err)
	}

	var mergedRecords []RootaraRecord

	// 根据方法选择不同的读取函数
	switch method {
	case "23andme":
		mergedRecords, err = read23andmeResult(inputPath, rootaraRecords)
	case "ancestry":
		mergedRecords, err = readAncestryResult(inputPath, rootaraRecords)
	case "wegene":
		mergedRecords, err = readUniResult(inputPath, rootaraRecords)
	default:
		return fmt.Errorf("不支持的方法: %s", method)
	}

	if err != nil {
		return fmt.Errorf("读取输入文件失败: %v", err)
	}

	// 创建输出文件
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("创建输出文件失败: %v", err)
	}
	defer outputFile.Close()

	// 创建CSV写入器
	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	// 写入标题行
	header := []string{"Chrom", "Start", "Ref", "Alt", "Gene", "RSID", "gnomAD_AF", "CLNSIG", "CLNDN", "Genotype", "Check"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("写入标题行失败: %v", err)
	}

	// 写入数据
	for _, record := range mergedRecords {
		row := []string{
			record.Chrom,
			record.Start,
			record.Ref,
			record.Alt,
			record.Gene,
			record.RSID,
			record.GnomAD_AF,
			record.CLNSIG,
			record.CLNDN,
			record.Genotype,
			record.Check,
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("写入数据行失败: %v", err)
		}
	}

	return nil
}

func main() {
	// 解析命令行参数
	inputPtr := flag.String("input", "", "输入文件路径")
	outputPtr := flag.String("output", "", "输出文件路径")
	methodPtr := flag.String("method", "23andme", "文件来源 (23andme/ancestry/wegene)")
	rootaraPtr := flag.String("rootara", "Rootara.core.202404.txt.gz", "Rootara核心库文件路径")

	flag.Parse()

	// 检查必需参数
	if *inputPtr == "" || *outputPtr == "" || *rootaraPtr == "" {
		flag.Usage()
		os.Exit(1)
	}

	// 检查方法是否有效
	validMethods := map[string]bool{
		"23andme":  true,
		"ancestry": true,
		"wegene":   true,
	}
	if !validMethods[*methodPtr] {
		fmt.Printf("不支持的方法: %s\n", *methodPtr)
		flag.Usage()
		os.Exit(1)
	}

	// 确保输出目录存在
	outputDir := filepath.Dir(*outputPtr)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Printf("创建输出目录失败: %v\n", err)
		os.Exit(1)
	}

	// 创建CSV文件
	if err := csvCreate(*inputPtr, *outputPtr, *methodPtr, *rootaraPtr); err != nil {
		fmt.Printf("处理失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("处理完成，结果已保存到:", *outputPtr)
}