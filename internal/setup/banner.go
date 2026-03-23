package setup

import "fmt"

const bannerArt = "\n" +
	"    ____  _ ____                __\n" +
	"   / __ )(_) __/________ ______/ /_\n" +
	"  / __  / / /_/ ___/ __ `/ ___/ __/\n" +
	" / /_/ / / __/ /  / /_/ / /__/ /_\n" +
	"/_____/_/_/ /_/   \\__,_/\\___/\\__/"

func PrintBanner() {
	fmt.Println(TitleStyle.Render(bannerArt))
	fmt.Println()
	fmt.Println(SubtitleStyle.Render("Log Management, Detection, and Collaboration"))
	fmt.Println(DimStyle.Render(fmt.Sprintf("  Version: %s", Version)))
	fmt.Println()
}
