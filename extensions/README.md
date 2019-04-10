How to create an extension
-----
1. Folder with your extension should be placed in directory __extensions__
2. It has to include package main
3. There should be a type with an implementation of interface Extension
4. And variable of this type should be available for lookup
```
type Extension interface {
	RegisterCommands(cmd *cobra.Command)
	GetAllowedConfigKeys() map[string]*string
}
  ```

How to include extension to project
-----
1. You should compile folder __extensions/name_of_extension_folder__ with options
``--buildmode=plugin -o /path/to/name_of_extension.so``
2. And place __.so__ in folder __extensions/build__

