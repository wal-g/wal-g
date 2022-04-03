package common

import (
	"os"

	"github.com/spf13/cobra"
)

const (
	completionShort = "Output shell completion code for the specified shell"

	completionLong = `Output shell completion code for the specified shell. The shell code must be evaluated
to provide interactive completion of wal-g commands. Note that wal-g have to be
configured properly for autocomplete to work`

	completionExample = `  Bash:
    If bash-completion is not installed on Linux, install the 'bash-completion' package
    via your distribution's package manager. Write bash completion code to .bashrc and then source it:
      echo 'source <(wal-g completion bash)' >>~/.bashrc
      source ~/.bashrc
  Zsh:
    If shell completion is not already enabled in your environment, you will need to enable it.
    You can execute the following once:
      echo "autoload -U compinit; compinit" >> ~/.zshrc
    To load completions for each session, execute once:
      wal-g completion zsh > ${fpath[1]}/_wal-g`
)

// completionCmd represents the completion command
var CompletionCmd = &cobra.Command{
	Use:       "completion bash|zsh",
	Short:     completionShort,
	Long:      completionLong,
	Example:   completionExample,
	ValidArgs: []string{"bash", "zsh"},
	Args:      cobra.ExactValidArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		switch args[0] {
		case "bash":
			cmd.Root().GenBashCompletionV2(os.Stdout, true)
		case "zsh":
			cmd.Root().GenZshCompletion(os.Stdout)
		}
	},
}

func init() {
	// fix to disable the required settings check for the completion subcommand
	CompletionCmd.PersistentPreRun = func(*cobra.Command, []string) {}
}
