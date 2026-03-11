package setup

import "fmt"

const bannerArt = `
    _____ _______ _____  _______   __
   / ____|__   __|  __ \|_   _\ \ / /
  | (___    | |  | |__) | | |  \ V /
   \___ \   | |  |  _  /  | |   > <
   ____) |  | |  | | \ \ _| |_ / . \
  |_____/   |_|  |_|  \_\_____/_/ \_\`

func PrintBanner() {
	fmt.Println(TitleStyle.Render(bannerArt))
	fmt.Println()
	fmt.Println(SubtitleStyle.Render("Log Management, Detection, and Collaboration"))
	fmt.Println(DimStyle.Render(fmt.Sprintf("  Version: %s", Version)))
	fmt.Println()
}
